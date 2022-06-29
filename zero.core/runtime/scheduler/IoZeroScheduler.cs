using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
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
            Zero = new IoZeroScheduler(Default, native:true); 
            ZeroDefault = Zero;
            //ZeroDefault = Default; //TODO: Uncomment to enable native .net scheduler...
            Zero.InitQueues();
        }

        public IoZeroScheduler(TaskScheduler fallback, CancellationTokenSource asyncTasks = null, bool native = false)
        {
            _ = base.Id; // force ID creation of the default scheduler

            if (!Enabled)
                return;

            _fallbackScheduler = fallback;
            _asyncTasks = asyncTasks ?? new CancellationTokenSource();
            _workerCount = Math.Max(Environment.ProcessorCount * 2, 8);
            _asyncCount = _workerCount * 2;
            _asyncTaskWithContextCount = _asyncCount * 8;
            _syncCount = _forkCount = _asyncFallbackCount =_asyncCount;
            var capacity = MaxWorker + 1;

            //TODO: F
            var asyncContinue = false;
            _taskChannel = new IoZeroSemaphoreChannel<Task>(string.Empty, short.MaxValue /3, 0, true); //TRUE
            _asyncForkQueue = new IoZeroSemaphoreChannel<Func<ValueTask>>(string.Empty, short.MaxValue / 3, 0, asyncContinue);
            _asyncCallbackQueue = new IoZeroSemaphoreChannel<ZeroContinuation>(string.Empty, short.MaxValue / 3, 0, asyncContinue);//not used
            _asyncFallbackQueue = new IoZeroSemaphoreChannel<ZeroContinuation>(string.Empty, short.MaxValue / 3, 0, true);
            _asyncValueTaskQueue = new IoZeroSemaphoreChannel<ZeroValueContinuation>(string.Empty, short.MaxValue / 3, 0, true);
            _asyncValueTaskWithContextQueue = new IoZeroSemaphoreChannel<ZeroValueContinuation>(string.Empty, short.MaxValue / 3, 0, true);
            _forkQueue = new IoZeroSemaphoreChannel<Action>(string.Empty, short.MaxValue / 3, 0, true);
            
            _callbackHeap = new IoHeap<ZeroContinuation>(string.Empty, short.MaxValue << 1, (_, _) => new ZeroContinuation())
            {
                PopAction = (signal, _) =>
                {
                    signal.Callback = null;
                    signal.State = null;
                    signal.Timestamp = Environment.TickCount;
                }
            };

            _diagnosticsHeap = new IoHeap<List<int>>(string.Empty, short.MaxValue << 1, (context, _) => new List<int>(context is int i ? i : 0))
            {
                PopAction = (list, _) =>
                {
                    list.Clear();
                }
            };

            _contextHeap = new IoHeap<ZeroValueContinuation>(string.Empty, short.MaxValue << 1,
                (_, _) => new ZeroValueContinuation())
            {
                PopAction = (valueTask, _) =>
                {
                    valueTask.Timestamp = Environment.TickCount;
                }
            };

            _native = native;
        }

        private void InitQueues()
        {
            //callbacks
            for (var i = 0; i < _workerCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.HandleAsyncSchedulerTask(i).FastPath().ConfigureAwait(false);
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.HideScheduler, Default);
            }


            //callbacks
            for (var i = 0; i < _syncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)(state);
                    await @this.HandleAsyncCallback(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //callbacks
            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)(state);
                    await @this.HandleAsyncValueTask(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //callbacks
            for (var i = 0; i < _asyncTaskWithContextCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)(state);
                    await @this.HandleAsyncValueTaskWithContext(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //async callbacks
            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.ForkAsyncCallbacks(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //forks
            for (var i = 0; i < _forkCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.ForkCallbacks(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //forks with context
            for (var i = 0; i < _asyncFallbackCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.AsyncFallbacks(i).FastPath().ConfigureAwait(false);
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, Default);
            }
        }

        internal class ZeroContinuation
        {
            public Action<object> Callback;
            public object State;
            public int Timestamp;
        }

        internal class ZeroValueContinuation
        {
            public ValueTask ValueTask;
            public Func<object, ValueTask> ValueFunc;
            public object Context;
            public int Timestamp;
        }

        [Flags]
        enum IoMark
        {
            Undefined = 0,
            Worker = 1,
            Queen = 1 << 1
        }

        //TODO: <<--- these values need more research. It is not at all clear why
        //TODO: thread lockups happen when you change these values.Too high and you get CPU flat-lining without any work being done. To little, deadlock! How to tune is unclear?

        //The rate at which the scheduler will be allowed to "burst" allowing per tick unchecked new threads to be spawned until one of them spawns
        private static readonly int WorkerSpawnBurstTimeMs = 1000;
        private static readonly int WorkerSpawnPassThrough = 3;
        //The maximum burst rate per WorkerSpawnBurstTimeMs tick
        private static readonly int WorkerSpawnBurstMax = Math.Max(Environment.ProcessorCount, WorkerSpawnPassThrough * WorkerSpawnPassThrough) / WorkerSpawnPassThrough * WorkerSpawnPassThrough;
        //The load threshold at which more workers are added
        private static readonly double WorkerSpawnThreshold = 0.7;


        private static volatile int _workerSpawnBurstMax = WorkerSpawnBurstMax; 
        private static readonly int MaxWorker = short.MaxValue / 3;
        public static readonly TaskScheduler ZeroDefault;
        public static readonly IoZeroScheduler Zero;
        private readonly CancellationTokenSource _asyncTasks;
        private readonly IoZeroSemaphoreChannel<Task> _taskChannel;

        private readonly IoZeroSemaphoreChannel<Func<ValueTask>> _asyncForkQueue;
        private readonly IoZeroSemaphoreChannel<ZeroContinuation> _asyncCallbackQueue;
        private readonly IoZeroSemaphoreChannel<ZeroContinuation> _asyncFallbackQueue;
        private readonly IoZeroSemaphoreChannel<ZeroValueContinuation> _asyncValueTaskQueue;
        private readonly IoZeroSemaphoreChannel<ZeroValueContinuation> _asyncValueTaskWithContextQueue;
        private readonly IoZeroSemaphoreChannel<Action> _forkQueue;
        private readonly IoHeap<ZeroContinuation> _callbackHeap;
        private readonly IoHeap<ZeroValueContinuation> _contextHeap;
        private readonly IoHeap<List<int>> _diagnosticsHeap;

        private volatile int _workerLoad;
        public int Load => _workerLoad;
        private volatile int _asyncLoad;
        public int AsyncLoad => _asyncLoad;
        private volatile int _forkLoad;
        public int ForkLoad => _forkLoad;
        public double QTime => (double)_taskQTime / _completedQItemCount;
        public double AQTime => (double)_asyncQTime / _completedAsyncCount;

        private volatile int _lastWorkerSpawnedTime;
        private volatile int _workerCount;
        private volatile int _asyncCount;
        private volatile int _asyncTaskWithContextCount;
        private volatile int _syncCount;
        private volatile int _forkCount;
        private volatile int _asyncFallbackCount;
        private long _completedWorkItemCount;
        private long _completedQItemCount;
        private long _completedForkAsyncCount;
        private long _completedAsyncCount;
        private long _asyncQTime;
        private long _taskQTime;
        private long _completedForkCount;
        //private long _callbackCount;
        private readonly TaskScheduler _fallbackScheduler;
        private bool _native;

        public int RLength => _taskChannel.ReadyCount;

        public int QLength => _taskChannel.WaitCount;
        public int ThreadCount => _workerCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public long CompletedQItemCount => _completedWorkItemCount;
        public long CompletedAsyncCount => _completedAsyncCount;
        public double LoadFactor => (double)Load / _workerCount;
        public long Capacity => _workerCount;
        private async ValueTask ForkAsyncCallbacks(int threadIndex)
        {
            //await foreach (var job in _asyncForkQueue.PumpOnConsumeAsync(threadIndex).ConfigureAwait(false))
            while (!_asyncTasks.IsCancellationRequested)
            {
                
                try
                {
                    var job = await _asyncForkQueue.WaitAsync().FastPath().ConfigureAwait(false);
                    await job().FastPath();
                    Interlocked.Increment(ref _completedForkAsyncCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        private async ValueTask ForkCallbacks(int threadIndex)
        {
            //await foreach (var job in _forkQueue.PumpOnConsumeAsync(threadIndex))
            while (!_asyncTasks.IsCancellationRequested)
            {
                try
                {
                    var job = await _forkQueue.WaitAsync().FastPath();
                    Interlocked.Increment(ref _forkLoad);
                    job();
                    Interlocked.Increment(ref _completedForkCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _forkLoad);
                }
            }
        }

        private async ValueTask AsyncFallbacks(int threadIndex)
        {
            //await foreach (var job in _asyncFallbackQueue.Reader.ReadAllAsync().ConfigureAwait(false))
            while (!_asyncTasks.IsCancellationRequested)
            {
                ZeroContinuation job = null;
                try
                {
                    job = await _asyncFallbackQueue.WaitAsync().FastPath().ConfigureAwait(false);
                    job.Callback(job.State);
                    Interlocked.Increment(ref _completedForkCount);
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

        private async ValueTask HandleAsyncSchedulerTask(int threadIndex)
        {
            //await foreach (var job in _taskQueue.Reader.ReadAllAsync().ConfigureAwait(false))

            while (!_asyncTasks.IsCancellationRequested)
            {
                try
                {
                    var job = await _taskChannel.WaitAsync().FastPath().ConfigureAwait(false);
                    if (job.Status <= TaskStatus.WaitingToRun)
                    {
                        Interlocked.Increment(ref _workerLoad);
                        try
                        {
                            if (!TryExecuteTask(job)) //&& job.Status <= TaskStatus.Running)
                                LogManager.GetCurrentClassLogger().Fatal(
                                    $"{nameof(HandleAsyncSchedulerTask)}: Unable to execute task, id = {job.Id}, state = {job.Status}, async-state = {job.AsyncState}, success = {job.IsCompletedSuccessfully}");
                            else
                                Interlocked.Increment(ref _completedWorkItemCount);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _workerLoad);
                        }
                    }
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        private async ValueTask HandleAsyncCallback(int threadIndex)
        {
            //await foreach (var job in _asyncCallbackQueue.BalanceOnConsumeAsync(threadIndex).ConfigureAwait(_native))
            while (!_asyncTasks.IsCancellationRequested)
            {
                ZeroContinuation job = null;
                try
                {
                    job = await _asyncCallbackQueue.WaitAsync().FastPath().ConfigureAwait(_native);
                    job.Callback(job.State);
                    Interlocked.Increment(ref _completedAsyncCount);
                    Interlocked.Add(ref _asyncQTime, job.Timestamp.ElapsedMs());

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
        private async ValueTask HandleAsyncValueTask(int threadIndex)
        {
            //await foreach (var job in _asyncQueue.BalanceOnConsumeAsync(threadIndex).ConfigureAwait(_native))
            while (!_asyncTasks.IsCancellationRequested)
            {
                ZeroValueContinuation job = null;
                try
                {
                    job = await _asyncValueTaskQueue.WaitAsync().FastPath().ConfigureAwait(_native);
                    Debug.Assert(TaskScheduler.Current.Id == IoZeroScheduler.Zero.Id);
                    await job.ValueTask.FastPath();
                    Interlocked.Increment(ref _completedAsyncCount);
                    Interlocked.Add(ref _asyncQTime, job.Timestamp.ElapsedMs());

                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    _contextHeap.Return(job);
                }
            }
        }

        private async ValueTask HandleAsyncValueTaskWithContext(int threadIndex)
        {
            //await foreach (var job in _asyncValueTaskQueue.Reader.ReadAllAsync().ConfigureAwait(_native))
            while (!_asyncTasks.IsCancellationRequested)
            {
                ZeroValueContinuation job = null;
                try
                {
                    job = await _asyncValueTaskWithContextQueue.WaitAsync().FastPath().ConfigureAwait(_native);
                    Interlocked.Increment(ref _asyncLoad);
                    //Debug.Assert(TaskScheduler.Current.Id == IoZeroScheduler.Zero.Id);
                    await job.ValueFunc(job.Context).FastPath();
                    Interlocked.Increment(ref _completedAsyncCount);
                    Interlocked.Add(ref _asyncQTime, job.Timestamp.ElapsedMs());
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _asyncLoad);
                    _contextHeap.Return(job);
                }
            }
        }

        /// <summary>
        /// Gets a list of tasks held by this scheduler
        /// </summary>
        /// <returns></returns>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            var l = new List<Task>();
            //while (_taskChannel.)
            //{
            //    l.Add(item);
            //}
            return l;
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
            var ts = Environment.TickCount;
            
            if (_taskChannel.Release(task) < 0)
            {
                //TODO: What is this bug on getting a task.id = 1? How does it get here? Debugging it heisenbugs... 
                //if (_taskChannel.Release(task) == 0 && !TryExecuteTaskInline(task, false))
                    throw new InternalBufferOverflowException($"{nameof(_taskChannel)}: count = {_taskChannel.WaitCount}, capacity {Capacity}");
            }

            Interlocked.Add(ref _taskQTime, ts.ElapsedMs());
            Interlocked.Increment(ref _completedQItemCount);

            //insane checks
            if (LoadFactor > WorkerSpawnThreshold && _lastWorkerSpawnedTime.ElapsedMs() > WorkerSpawnBurstTimeMs && _workerCount < short.MaxValue / WorkerSpawnPassThrough || LoadFactor >= 0.99)
            {
                FallbackContext(static state =>
                {
                    var @this = (IoZeroScheduler)state;
                    if (Interlocked.Decrement(ref _workerSpawnBurstMax) > 0 || @this.LoadFactor > WorkerSpawnThreshold)
                    {
                        _ = Task.Factory.StartNew(static async state =>
                            {
                                var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                                await @this.HandleAsyncSchedulerTask(i).FastPath().ConfigureAwait(false);
                            }, (@this, Interlocked.Increment(ref @this._workerCount) - 1), CancellationToken.None,
                            TaskCreationOptions.LongRunning, Default);

                        _ = Task.Factory.StartNew(static async state =>
                            {
                                var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                                await @this.HandleAsyncValueTaskWithContext(i).FastPath();
                            }, (@this, Interlocked.Increment(ref @this._asyncTaskWithContextCount)),
                            CancellationToken.None,
                            TaskCreationOptions.LongRunning, ZeroDefault);

                        if (_workerSpawnBurstMax == 0)
                            Interlocked.Exchange(ref _workerSpawnBurstMax, WorkerSpawnBurstMax);

                        @this._lastWorkerSpawnedTime = Environment.TickCount;
                        Interlocked.MemoryBarrier();
                        Console.WriteLine(
                            $"Adding zero thread {@this._workerCount}, load = {@this.LoadFactor * 100:0.0}%");
                    }
                }, this);
            }
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
                result = _asyncCallbackQueue.Release(handler);
                return result > 0;
            }
            finally
            {
                if (result <= 0 && handler != null)
                    _callbackHeap.Return(handler);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncContext(Func<object,ValueTask> valueTask, object context)
        {
            var c = _contextHeap.Take();
            if (c == null) throw new OutOfMemoryException(nameof(LoadAsyncContext));
            c.ValueFunc = valueTask;
            c.Context = context;
            return _asyncValueTaskWithContextQueue.Release(c) != 0;
        }

        //API
        public void TryExecuteTaskInline(Task task) => TryExecuteTaskInline(task, false);
        public void Queue(Task task) => QueueTask(task);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(ValueTask task)
        {
            var c = _contextHeap.Take();
            if (c == null) throw new OutOfMemoryException(nameof(LoadAsyncCallback));
            c.ValueTask = task;
            return _asyncValueTaskQueue.Release(c) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(Func<ValueTask> callback) => _asyncForkQueue.Release(callback) != 0;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public bool LoadExclusiveZone(Func<object,ValueTask> callback, object state = null) => _exclusiveQueue.TryEnqueue(callback) > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Fork(Action callback) =>_forkQueue.Release(callback) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool FallbackContext(Action<object> callback, object context = null)
        {
            var qItem = _callbackHeap.Take();
            if (qItem == null) throw new OutOfMemoryException(nameof(FallbackContext));
            qItem.Callback = callback;
            qItem.State = context;
            return _asyncFallbackQueue.Release(qItem) != 0;
        }
    }
}
