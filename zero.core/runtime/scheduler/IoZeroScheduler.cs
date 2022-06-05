using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

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
            _workerCount = Math.Max(Environment.ProcessorCount * 2, 4);
            //_workerCount = 10;
            _asyncCount = _workerCount * 4;
            _syncCount = _forkCount = _forkContextCount =_asyncCount;
            var capacity = MaxWorker + 1;

            //TODO: tuning
            //_taskQueue = new IoZeroQ<Task>(string.Empty, short.MaxValue / 3, false, _asyncTasks, MaxWorker - 1, zeroAsyncMode: true);
            _taskQueue = Channel.CreateBounded<Task>(new BoundedChannelOptions(_workerCount * 1024)
            {
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            });//TODO: tuning

            _asyncCallbackQueue = new IoZeroQ<ZeroContinuation>(string.Empty, short.MaxValue / 3, false, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);
            _asyncQueue = new IoZeroQ<ZeroValueContinuation>(string.Empty, short.MaxValue / 3, false, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);
            _asyncForkQueue = new IoZeroQ<Func<ValueTask>>(string.Empty, short.MaxValue / 3, false, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);
            //_asyncContextQueue = new IoZeroQ<ZeroValueContinuation>(string.Empty, short.MaxValue / 3, false, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);
            _asyncContextQueue = Channel.CreateBounded<ZeroValueContinuation>(new BoundedChannelOptions(_workerCount * 1024)
            {
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            });//TODO: tuning

            _forkContextQueue = Channel.CreateBounded<ZeroContinuation>(new BoundedChannelOptions(_workerCount * 1024)
            {
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            });//TODO: tuning


            _callbackHeap = new IoHeap<ZeroContinuation>(string.Empty, short.MaxValue << 1, (_, _) => new ZeroContinuation(), true)
            {
                PopAction = (signal, _) =>
                {
                    signal.Callback = null;
                    signal.State = null;
                    signal.Timestamp = Environment.TickCount;
                }
            };

            _diagnosticsHeap = new IoHeap<List<int>>(string.Empty, short.MaxValue << 1, (context, _) => new List<int>(context is int i ? i : 0), true)
            {
                PopAction = (list, _) =>
                {
                    list.Clear();
                }
            };

            _asyncHeap = new IoHeap<ZeroValueContinuation>(string.Empty, short.MaxValue << 1,
                (_, _) => new ZeroValueContinuation(), true)
            {
                PopAction = (valueTask, _) =>
                {
                    valueTask.Timestamp = Environment.TickCount;
                }
            };

            _forkContextHeap = new IoHeap<ZeroContinuation>(string.Empty, short.MaxValue << 1,
                (_, _) => new ZeroContinuation(), true)
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
                    await @this.LoadTask(i).FastPath().ConfigureAwait(false);
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.HideScheduler, Default);
            }

            //callbacks
            for (var i = 0; i < _syncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)(state);
                    await @this.AsyncCallbacks(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //callbacks
            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)(state);
                    await @this.AsyncValueTasks(i).FastPath();
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
            }

            //callbacks
            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)(state);
                    await @this.AsyncValueContextTasks(i).FastPath();
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
            for (var i = 0; i < _forkContextCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.ForkContextCallbacks(i).FastPath().ConfigureAwait(false);
                }, (this, i), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);
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
        private readonly Channel<Task> _taskQueue;
        private readonly IoZeroQ<Func<ValueTask>> _asyncForkQueue;
        private readonly Channel<ZeroValueContinuation> _asyncContextQueue;
        private readonly IoZeroQ<ZeroValueContinuation> _asyncQueue;
        private readonly IoZeroQ<Action> _forkQueue;
        private readonly Channel<ZeroContinuation> _forkContextQueue;
        private readonly IoZeroQ<ZeroContinuation> _asyncCallbackQueue;
        private readonly IoHeap<ZeroContinuation> _callbackHeap;
        private readonly IoHeap<ZeroValueContinuation> _asyncHeap;
        private readonly IoHeap<ZeroContinuation> _forkContextHeap;
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
        private volatile int _syncCount;
        private volatile int _forkCount;
        private volatile int _forkContextCount;
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

        public int WLength => _taskQueue?.Reader.Count ?? 0;

        public int QLength => _taskQueue?.Reader.Count ?? 0;
        public int ThreadCount => _workerCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public long CompletedQItemCount => _completedWorkItemCount;
        public long CompletedAsyncCount => _completedAsyncCount;
        public double LoadFactor => (double)Load / _workerCount;
        public long Capacity => _workerCount;
        private async ValueTask ForkAsyncCallbacks(int threadIndex)
        {
            await foreach (var job in _asyncForkQueue.PumpOnConsumeAsync(threadIndex).ConfigureAwait(false))
            {
                try
                {
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
            await foreach (var job in _forkQueue.PumpOnConsumeAsync(threadIndex))
            {
                try
                {
                    job();
                    Interlocked.Increment(ref _completedForkCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        private async ValueTask ForkContextCallbacks(int threadIndex)
        {
            await foreach (var job in _forkContextQueue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
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

        private async ValueTask LoadTask(int threadIndex)
        {
            await foreach (var job in _taskQueue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    if (job.Status <= TaskStatus.WaitingToRun)
                    {
                        Interlocked.Increment(ref _workerLoad);
                        try
                        {
                            if (!TryExecuteTask(job)) //&& job.Status <= TaskStatus.Running)
                                LogManager.GetCurrentClassLogger().Fatal($"{nameof(LoadTask)}: Unable to execute task, id = {job.Id}, state = {job.Status}, async-state = {job.AsyncState}, success = {job.IsCompletedSuccessfully}");
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

        private async ValueTask AsyncCallbacks(int threadIndex)
        {
            await foreach (var job in _asyncCallbackQueue.BalanceOnConsumeAsync(threadIndex).ConfigureAwait(_native))
            {
                try
                {
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
        private async ValueTask AsyncValueTasks(int threadIndex)
        {
            await foreach (var job in _asyncQueue.BalanceOnConsumeAsync(threadIndex).ConfigureAwait(_native))
            {
                try
                {
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
                    _asyncHeap.Return(job);
                }
            }
        }

        private async ValueTask AsyncValueContextTasks(int threadIndex)
        {
            await foreach (var job in _asyncContextQueue.Reader.ReadAllAsync().ConfigureAwait(_native))
            {
                try
                {
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
                    _asyncHeap.Return(job);
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
            while (_taskQueue.Reader.TryRead(out var item))
            {
                l.Add(item);
            }
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
            if (!_taskQueue.Writer.TryWrite(task))
            {
                //TODO: What is this bug on getting a task.id = 1? How does it get here? Debugging it heisenbugs... 
                if (!_taskQueue.Writer.TryWrite(task) && !TryExecuteTaskInline(task, false))
                    throw new InternalBufferOverflowException($"{nameof(_taskQueue)}: count = {_taskQueue.Reader.Count}, capacity {Capacity}");
            }

            Interlocked.Add(ref _taskQTime, ts.ElapsedMs());
            Interlocked.Increment(ref _completedQItemCount);

            //insane checks
            if (LoadFactor > WorkerSpawnThreshold && _lastWorkerSpawnedTime.ElapsedMs() > WorkerSpawnBurstTimeMs && _workerCount < short.MaxValue / WorkerSpawnPassThrough)
            {
                ForkContext(static state =>
                {
                    var @this = (IoZeroScheduler)state;
                    Console.WriteLine($"Adding zero thread {@this._workerCount}, load = {@this.LoadFactor * 100:0.0}%");
                    int slot;
                    if ((slot = Interlocked.Decrement(ref _workerSpawnBurstMax)) > 0 && slot % 3 == 0)
                    {
                        _ = Task.Factory.StartNew(static async state =>
                        {
                            var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                            await @this.LoadTask(i).FastPath();
                        }, (@this, Interlocked.Increment(ref @this._workerCount) - 1), CancellationToken.None, TaskCreationOptions.LongRunning, Default);

                        _ = Task.Factory.StartNew(static async state =>
                        {
                            var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                            await @this.AsyncValueContextTasks(i).FastPath();
                        }, (@this, Interlocked.Increment(ref @this._asyncCount)), CancellationToken.None, TaskCreationOptions.LongRunning, ZeroDefault);

                        _workerSpawnBurstMax = WorkerSpawnBurstMax;
                        @this._lastWorkerSpawnedTime = Environment.TickCount;
                        Interlocked.MemoryBarrier();
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
                Interlocked.Increment(ref _forkLoad);
                handler = _callbackHeap.Take();
                if (handler == null) return false;

                handler.Callback = callback;
                handler.State = state;
                result = _asyncCallbackQueue.TryEnqueue(handler);
                return result > 0;
            }
            finally
            {
                Interlocked.Decrement(ref _forkLoad);
                if (result <= 0 && handler != null)
                    _callbackHeap.Return(handler);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncContext(Func<object,ValueTask> valueTask, object context)
        {
            var c = _asyncHeap.Take();
            if (c == null) throw new OutOfMemoryException(nameof(LoadAsyncContext));
            c.ValueFunc = valueTask;
            c.Context = context;
            return _asyncContextQueue.Writer.TryWrite(c);
        }

        //API
        public void TryExecuteTaskInline(Task task) => TryExecuteTaskInline(task, false);
        public void Queue(Task task) => QueueTask(task);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(ValueTask task)
        {
            var c = _asyncHeap.Take();
            if (c == null) throw new OutOfMemoryException(nameof(LoadAsyncCallback));
            c.ValueTask = task;
            while (_asyncQueue.TryEnqueue(c) <= 0) { };
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(Func<ValueTask> callback, object state = null) => _asyncForkQueue.TryEnqueue(callback) > 0;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public bool LoadExclusiveZone(Func<object,ValueTask> callback, object state = null) => _exclusiveQueue.TryEnqueue(callback) > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Fork(Action callback, object state = null)
        {
            while (_forkQueue.TryEnqueue(callback) <= 0){};
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ForkContext(Action<object> callback, object context = null)
        {
            var qItem = _forkContextHeap.Take();
            if (qItem == null) throw new OutOfMemoryException(nameof(ForkContext));
            qItem.Callback = callback;
            qItem.State = context;
            while (!_forkContextQueue.Writer.TryWrite(qItem)) { };
            return true;
        }
    }
}
