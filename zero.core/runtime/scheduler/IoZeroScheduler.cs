using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;

namespace zero.core.runtime.scheduler
{
    /// <summary>
    /// Experimental task scheduler based on zero tech
    /// </summary>
    public class IoZeroScheduler : TaskScheduler
    {
        public static bool Enabled = true;
        static IoZeroScheduler()
        {
            Zero = new IoZeroScheduler(Default);
            Zero.InitQueues();
            ZeroDefault = Zero;
            //ZeroDefault = Default; //TODO: Uncomment to enable native .net scheduler...
        }

        public IoZeroScheduler(TaskScheduler fallback, CancellationTokenSource asyncTasks = null)
        {
            _ = base.Id; // force ID creation of the default scheduler
            
            if(!Enabled)
                return;

            _fallbackScheduler = fallback;
            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Math.Max(Environment.ProcessorCount << 2, 4);
            _queenCount = Math.Max(Environment.ProcessorCount >> 4, 1) + 1;
            _asyncCount = _workerCount>>1;
            _syncCount = _asyncCount;
            var capacity = MaxWorker + 1;

            //TODO: tuning
            _taskQueue = new IoZeroQ<Task>(string.Empty, capacity * 2, true, _asyncTasks, MaxWorker - 1, zeroAsyncMode:true);
            _callbackQueue = new IoZeroQ<ZeroContinuation>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: false);
            _callbackQueue = new IoZeroQ<ZeroContinuation>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: false);
            //_contextQueue = new IoZeroQ<ZeroContinuation>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: false);
            _asyncCallbackQueue = new IoZeroQ<Func<ValueTask>>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);
            //_exclusiveQueue = new IoZeroQ<Func<object,ValueTask>>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);
            _forkQueue = new IoZeroQ<Action>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);

            _callbackHeap = new IoHeap<ZeroContinuation>(string.Empty, capacity * 2, (_, _) => new ZeroContinuation(), true)
            {
                PopAction = (signal, _) =>
                {
                    signal.Callback = null;
                    signal.State = null;
                }
            };

            _diagnosticsHeap = new IoHeap<List<int>>(string.Empty, capacity, (context, _) => new List<int>(context is int i ? i : 0), true)
            {
                PopAction = (list, _) =>
                {
                    list.Clear();
                }
            };

            _workerPunchCards = new int[MaxWorker];
            Array.Fill(_workerPunchCards, -1);
            _queenPunchCards = new int[MaxWorker];
            Array.Fill(_queenPunchCards, -1);
        }

        private void InitQueues()
        {
            //callbacks
            for (var i = 0; i < _syncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.LoadTask(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
            }

            //callbacks
            for (var i = 0; i < _syncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.LoadCallback(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, this);
            }

            //async callbacks
            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.LoadAsyncCallback(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach, this);
            }

            ////exclusive callbacks
            //_ = Task.Factory.StartNew(static async state =>
            //{
            //    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
            //    await @this.LoadExclusiveZone(i).FastPath();
            //}, (this, 1), CancellationToken.None, TaskCreationOptions.DenyChildAttach, this);
        }

        internal class ZeroSignal
        {
            public volatile Task Task;
            public volatile int Processed = -1;
        }

        internal class ZeroContinuation
        {
            public volatile Action<object> Callback;
            public volatile object State;
        }

        [Flags]
        enum IoMark{
            Undefined = 0,
            Worker = 1,
            Queen = 1 << 1
        }

        //The rate at which the scheduler will be allowed to "burst" allowing per tick unchecked new threads to be spawned until one of them spawns
        private static readonly int WorkerSpawnBurstTimeMs = 100;
        private static readonly int MaxWorker = short.MaxValue>>1;
        public static readonly TaskScheduler ZeroDefault;       
        public static readonly IoZeroScheduler Zero;
        private readonly CancellationTokenSource _asyncTasks;
        private readonly int[] _workerPunchCards;
        private readonly int[] _queenPunchCards;
        private readonly IoZeroQ<Task> _taskQueue;
        private readonly IoZeroQ<Func<ValueTask>> _asyncCallbackQueue;
        private readonly IoZeroQ<Action> _forkQueue;
        private readonly IoZeroQ<ZeroContinuation> _callbackQueue;
        private readonly IoHeap<ZeroContinuation> _callbackHeap;
        //private readonly IoZeroQ<Func<object,ValueTask>> _exclusiveQueue;
        private readonly IoHeap<List<int>> _diagnosticsHeap;

        private volatile int _workerCount;
        private volatile int _queenCount;
        private volatile int _asyncCount;
        private volatile int _syncCount;
        private long _completedWorkItemCount;
        private long _completedQItemCount;
        private long _completedAsyncCount;
        private long _callbackCount;
        private readonly TaskScheduler _fallbackScheduler;

        public List<int> Active
        {
            get
            {
                var l = _diagnosticsHeap.Take(_workerCount);
                if (l == null)
                    return Array.Empty<int>().ToList();

                for (var i = 0; i < _workerCount; i++)
                {
                    if(_workerPunchCards[i] >= 0)
                        l.Add(_workerPunchCards[i]);
                }

                return l;
            }
        }

        public List<int> Blocked
        {
            get
            {
                var l = _diagnosticsHeap.Take(_workerCount);
                if (l == null)
                    return Array.Empty<int>().ToList();

                for (var i = 0; i < _workerCount; i++)
                {
                    if (_workerPunchCards[i] > 0)
                        l.Add(_workerPunchCards[i]);
                }

                return l;
            }
        }
        
        public List<int> Free
        {
            get
            {
                var l = _diagnosticsHeap.Take(_workerCount);
                if (l == null)
                    return Array.Empty<int>().ToList();

                for (var i = 0; i < _workerCount; i++)
                {
                    if (_workerPunchCards[i] == 0)
                        l.Add(_workerPunchCards[i]);
                }

                return l;
            }
        }

        public List<int> QActive
        {
            get
            {
                var l = _diagnosticsHeap.Take(_queenCount);
                if (l == null)
                    return Array.Empty<int>().ToList();

                for (var i = 0; i < _queenCount; i++)
                {
                    if (_queenPunchCards[i] >= 0)
                        l.Add(_queenPunchCards[i]);
                }

                return l;
            }
        }
        public List<int> QBlocked
        {
            get
            {
                var l = _diagnosticsHeap.Take(_queenCount);
                if (l == null)
                    return Array.Empty<int>().ToList();
                for (var i = 0; i < _queenCount; i++)
                {
                    if (_queenPunchCards[i] != 0)
                        l.Add(_queenPunchCards[i]);
                }

                return l;
            }
        }
        public List<int> QFree
        {
            get
            {
                var l = _diagnosticsHeap.Take(_queenCount);
                if (l == null)
                    return Array.Empty<int>().ToList();
                for (var i = 0; i < _queenCount; i++)
                {
                    if (_queenPunchCards[i] == 0)
                        l.Add(_queenPunchCards[i]);
                }

                return l;
            }
        }

        public int Load
        {
            get
            {
                var z = Blocked;
                var c = z.Count;
                _diagnosticsHeap.Return(z);
                return c;
            }
        }

        public int QLoad
        {
            get
            {
                var qBlocked = QBlocked;
                var c = qBlocked.Count;
                _diagnosticsHeap.Return(qBlocked);
                return c;
            }
        }

        public int WLength => _taskQueue?.Count??0;

        public int QLength => _taskQueue?.Count??0;
        public int ThreadCount => _workerCount;
        public int QThreadCount => _queenCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public long CompletedQItemCount => _completedWorkItemCount;

        public long CompletedAsyncCount => _completedAsyncCount;
        public double LoadFactor => (double) Load / _workerCount;
        public double QLoadFactor => (double) QLoad / _queenCount;
        public long Capacity => _taskQueue.Capacity;
        private async ValueTask LoadAsyncCallback(int threadIndex)
        {
            await foreach (var job in _asyncCallbackQueue.PumpOnConsumeAsync(threadIndex))
            {
                try
                {
                    await job().FastPath();
                    Interlocked.Increment(ref _completedAsyncCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        //private async ValueTask LoadExclusiveZone(int threadIndex)
        //{
        //    await foreach (var job in _exclusiveQueue.BalanceOnConsumeAsync(threadIndex))
        //    {
        //        try
        //        {
        //            await job().FastPath();
        //            Interlocked.Increment(ref _completedAsyncCount);
        //        }
        //        catch (Exception e)
        //        {
        //            LogManager.GetCurrentClassLogger().Trace(e);
        //        }
        //    }
        //}

        private async ValueTask Fork(int threadIndex)
        {
            await foreach (var job in _forkQueue.PumpOnConsumeAsync(threadIndex))
            {
                try
                {
                    job();
                    Interlocked.Increment(ref _completedAsyncCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        private async ValueTask LoadTask(int threadIndex)
        {
            await foreach (var job in _taskQueue.PumpOnConsumeAsync(threadIndex))
            {
                try
                {
                    if (job.Status == TaskStatus.WaitingToRun)
                    {
                        if (!TryExecuteTask(job))
                            LogManager.GetCurrentClassLogger().Fatal($"{nameof(LoadTask)}: Unable to execute task, id = {job.Id}, state = {job.Status}, async-state = {job.AsyncState}, success = {job.IsCompletedSuccessfully}");
                        else
                            Interlocked.Increment(ref _completedWorkItemCount);
                    }
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        private async ValueTask LoadCallback(int threadIndex)
        {
            await foreach (var job in _callbackQueue.BalanceOnConsumeAsync(threadIndex))
            {
                try
                {
                    job.Callback(job.State);
                    Interlocked.Increment(ref _completedAsyncCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    _callbackHeap.Return(job);
                }
            }
        }

        //private async ValueTask SpawnContext(int threadIndex)
        //{
        //    await foreach (var job in _contextQueue.PumpOnConsumeAsync(threadIndex).ConfigureAwait(false))
        //    {
        //        try
        //        {
        //            job.Callback(job.State);
        //        }
        //        catch (Exception e)
        //        {
        //            LogManager.GetCurrentClassLogger().Trace(e);
        //        }
        //        finally
        //        {
        //            _callbackHeap.Return(job);
        //        }
        //    }
        //}

        /// <summary>
        /// Gets a list of tasks held by this scheduler
        /// </summary>
        /// <returns></returns>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _taskQueue;
        }

        /// <summary>
        /// Queue this task to the scheduler
        /// </summary>
        /// <param name="task">The task</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void QueueTask(Task task)
        {
#if _TRACE_
            Console.WriteLine($"<--- Queueing task id = {task.Id}, {task.Status}");
#endif
            //queue the work for processing
            if (_taskQueue.TryEnqueue(task) <= 0)
            {
                if (_taskQueue.TryEnqueue(task) <= 0 && !TryExecuteTaskInline(task, false))
                    throw new InternalBufferOverflowException($"{nameof(_taskQueue)}: count = {_taskQueue.Count}, capacity {_taskQueue.Capacity}");
            }
            Interlocked.Increment(ref _completedQItemCount);
        }

        /// <summary>Tries to execute the task synchronously on this scheduler.</summary>
        /// <param name="task">The task to execute.</param>
        /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued to the scheduler.</param>
        /// <returns>true if the task could be executed; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return (!taskWasPreviouslyQueued) ?
                TryExecuteTask(task) :
                TryExecuteTaskInlineOnTargetScheduler(task, _fallbackScheduler);
        }

        /// <summary>
        /// Implements a reasonable approximation for TryExecuteTaskInline on the underlying scheduler,
        /// which we can't call directly on the underlying scheduler.
        /// </summary>
        /// <param name="task">The task to execute inline if possible.</param>
        /// <param name="target">Target scheduler</param>
        /// <returns>true if the task was inlined successfully; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryExecuteTaskInlineOnTargetScheduler(Task task, TaskScheduler target)
        {
            var t = new Task<bool>(static s =>
            {
                var tuple = (ValueTuple<IoZeroScheduler, Task>)s!;
                return tuple.Item1.TryExecuteTask(tuple.Item2);
            }, (this, task));
            try
            {
                t.RunSynchronously(target);
#pragma warning disable VSTHRD002 // Avoid problematic synchronous waits
                return t.Result;
#pragma warning restore VSTHRD002 // Avoid problematic synchronous waits
            }
            catch
            {
                _ = t.Exception;
                throw;
            }
            finally { t.Dispose(); }
        }

        /// <summary>
        /// returns a diagnostic result back into the heap
        /// </summary>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(List<int> value)
        {
            _diagnosticsHeap.Return(value);
        }

        public static void Dump()
        {
            foreach (var task in Zero._taskQueue)
            {
                if(task != null)
                    Console.WriteLine(task.AsyncState?.ToString());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool QueueCallback(Action<object> callback, object state)
        {
            ZeroContinuation handler = null;
            long result = -1;
            try
            {
                handler = _callbackHeap.Take();
                if (handler == null) return false;

                handler.Callback = callback;
                handler.State = state;
                result = _callbackQueue.TryEnqueue(handler);
                return result > 0;
            }
            finally
            {
                if(result <= 0 && handler != null)
                    _callbackHeap.Return(handler);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool QueueCallback2(Action<object> callback, object state)
        {
            ZeroContinuation handler = null;
            long result = -1;
            try
            {
                handler = _callbackHeap.Take();
                if (handler == null) return false;

                handler.Callback = callback;
                handler.State = state;
                result = _callbackQueue.TryEnqueue(handler);
                return result > 0;
            }
            finally
            {
                if (result <= 0 && handler != null)
                    _callbackHeap.Return(handler);
            }
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public bool QueueContext(Action<object> callback, object state)
        //{
        //    ZeroContinuation handler = null;
        //    long result = -1;
        //    try
        //    {
        //        handler = _callbackHeap.Take();
        //        if (handler == null) return false;

        //        handler.Callback = callback;
        //        handler.State = state;
        //        result = _contextQueue.TryEnqueue(handler);
        //        return result > 0;
        //    }
        //    finally
        //    {
        //        if (result <= 0 && handler != null)
        //            _callbackHeap.Return(handler);
        //    }
        //}

        //API
        public void TryExecuteTaskInline(Task task) => TryExecuteTaskInline(task, false);
        public void Queue(Task task) => QueueTask(task);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(Func<ValueTask> callback, object state = null) => _asyncCallbackQueue.TryEnqueue(callback) > 0;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public bool LoadExclusiveZone(Func<object,ValueTask> callback, object state = null) => _exclusiveQueue.TryEnqueue(callback) > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Fork(Action callback, object state = null) => _forkQueue.TryEnqueue(callback) > 0;

    }
}
