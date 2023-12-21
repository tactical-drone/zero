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
    public class IoZeroScheduler : TaskScheduler, IDisposable
    {
        static IoZeroScheduler()
        {
            _ = ZeroDefault;
        }
        public static bool Enabled = true;

//        public static IoZeroScheduler Default 
//        get {
//#if !NATIVE  //our experimental (ZERO GC PRESSURE) scheduler that provides insight (& stats) into bugs caused by bad async/await architecture
//            Zero = new IoZeroScheduler(Default, native: true);
//            ZeroDefault = Zero;
//            Zero.GrowQueues();
//#else       //Use the default .net scheduler. (this is probably the safer option for production)
//            Zero = new IoZeroScheduler(Default, native: false); 
//            ZeroDefault = Default; 
//            Zero.GrowQueues();
//#endif
//            return zero;
//        };

        public IoZeroScheduler(TaskScheduler fallback, CancellationTokenSource asyncTasks = null, bool native = false)
        {
            _ = base.Id; // force ID creation of the default scheduler

            if (!Enabled)
                return;

            _fallbackScheduler = fallback;
            _asyncTasks = asyncTasks ?? new CancellationTokenSource();
            //_taskQueueCapacity = Environment.ProcessorCount * 2;
            //_asyncCallbackWithContextCapacity = _asyncTaskCapacity = _asyncTaskWithContextCapacity = _taskQueueCapacity * 2;
            //_asyncFallbackCapacity = _forkCapacity = _asyncCallbackWithContextCapacity * 2;

            _taskQueueCapacity = Environment.ProcessorCount<<4;
            _asyncFallbackCapacity = _taskQueueCapacity>>1;

            _asyncTaskWithContextCapacity = _taskQueueCapacity >> 1;
            _asyncTaskCapacity = _taskQueueCapacity >> 1;

            _asyncCallbackWithContextCapacity = _taskQueueCapacity >> 1;

            _asyncForkCapacity = _taskQueueCapacity >> 1;
            _forkCapacity = _taskQueueCapacity >> 1;

            var size = _taskQueueCapacity * 4; //TODO: tuning
            _taskQueue = new IoZeroSemaphoreChannel<Task>($"{nameof(_taskQueue)}", size, 0, false);
            _asyncFallbackQueue = new IoZeroSemaphoreChannel<ZeroContinuation>($"{nameof(_asyncFallbackQueue)}", size, 0, false);

            _asyncTaskWithContextQueue = new IoZeroSemaphoreChannel<ZeroValueContinuation>($"{nameof(_asyncTaskWithContextQueue)}", size, 0, false);
            _asyncTaskQueue = new IoZeroSemaphoreChannel<ZeroValueContinuation>($"{nameof(_asyncTaskQueue)}", size, 0, false);

            _asyncCallbackWithContextQueue = new IoZeroSemaphoreChannel<ZeroContinuation>($"{nameof(_asyncCallbackWithContextQueue)}", size, 0, false);

            _asyncForkQueue = new IoZeroSemaphoreChannel<Func<ValueTask>>($"{nameof(_asyncForkQueue)}", size, 0, false);
            _forkQueue = new IoZeroSemaphoreChannel<Action>($"{nameof(_forkQueue)}", size, 0, false);
            
            var initialCap = size;
            _callbackHeap = new IoHeap<ZeroContinuation>($"{nameof(_callbackHeap)}", initialCap, (_, _) => new ZeroContinuation(), autoScale:true)
            {
                PopAction = (signal, _) =>
                {
                    signal.Timestamp = Environment.TickCount;
                }
            };

            //_diagnosticsHeap = new IoHeap<List<int>>($"{nameof(_diagnosticsHeap)}", initialCap, (context, _) => new List<int>(context is int i ? i : 0), autoScale: true)
            //{
            //    PopAction = (list, _) =>
            //    {
            //        list.Clear();
            //    }
            //};

            _contextHeap = new IoHeap<ZeroValueContinuation>($"{nameof(_contextHeap)}", initialCap,
                (_, _) => new ZeroValueContinuation(), autoScale: true)
            {
                PopAction = (valueTask, _) =>
                {
                    valueTask.Timestamp = Environment.TickCount;
                }
            };

            _native = native;
        }

        [Flags]
        enum IoMark
        {
            Undefined = 0,
            Worker = 1,
            Queen = 1 << 1
        }


        private const int InitExpected = 7;
        private int _initCount = 0;
        private int _initialized = 0;

        //TODO: <<--- these values need more research. It is not at all clear why
        //TODO: thread lockups happen when you change these values.Too high and you get CPU flat-lining without any work being done. To little, deadlock! How to tune is unclear?

        //The rate at which the scheduler will be allowed to "burst" allowing per tick unchecked new threads to be spawned until one of them spawns
        private static readonly int WorkerSpawnBurstTimeMs = 1000;
        private static readonly int WorkerSpawnPassThrough = 3;
        //The maximum burst rate per WorkerSpawnBurstTimeMs tick
        private static readonly int WorkerSpawnBurstMax = Math.Max(Environment.ProcessorCount, WorkerSpawnPassThrough * WorkerSpawnPassThrough) / WorkerSpawnPassThrough * WorkerSpawnPassThrough;
        //The load threshold at which more workers are added
        private static readonly double WorkerSpawnThreshold = 0.6;


        private static volatile int _workerSpawnBurstMax = WorkerSpawnBurstMax;

        private static TaskScheduler _zeroDefault;
        public static TaskScheduler ZeroDefault
        {
            get
            {
                if (_zeroDefault != null)
                    return _zeroDefault;

#if !NATIVE     //our experimental (ZERO GC PRESSURE) scheduler that provides insight (& stats) into bugs caused by bad async/await architecture
                _zeroDefault = Zero = new IoZeroScheduler(Default, native: true);
                Zero.GrowQueues();
#else           //Use the default .net scheduler. (this is probably the safer option for production)
                Zero = new IoZeroScheduler(Default, native: false);
                _zeroDefault = Default;
                Zero.GrowQueues();
#endif
                return _zeroDefault;
            }
        }

        public static IoZeroScheduler Zero;
        private readonly CancellationTokenSource _asyncTasks;

        private readonly IoZeroSemaphoreChannel<Task> _taskQueue;
        private readonly IoZeroSemaphoreChannel<ZeroContinuation> _asyncFallbackQueue;

        private readonly IoZeroSemaphoreChannel<ZeroValueContinuation> _asyncTaskWithContextQueue;
        private readonly IoZeroSemaphoreChannel<ZeroValueContinuation> _asyncTaskQueue;

        private readonly IoZeroSemaphoreChannel<ZeroContinuation> _asyncCallbackWithContextQueue;

        private readonly IoZeroSemaphoreChannel<Func<ValueTask>> _asyncForkQueue;
        private readonly IoZeroSemaphoreChannel<Action> _forkQueue;

        private readonly IoHeap<ZeroContinuation> _callbackHeap;
        private readonly IoHeap<ZeroValueContinuation> _contextHeap;
        //private readonly IoHeap<List<int>> _diagnosticsHeap;
        private ZeroContinuation _fbQQuickSlot;

        private int _taskQueueLoad;
        public int Load => _taskQueueLoad;

        private int _asyncFallBackLoad;
        public int AsyncFallBackLoad => _asyncFallBackLoad;

        private int _asyncTaskWithContextLoad;
        public int AsyncTaskWithContextLoad => _asyncTaskWithContextLoad;

        private int _asyncTaskLoad;
        public int AsyncTaskLoad => _asyncTaskLoad;

        private int _asyncCallbackWithContextLoad;
        public int AsyncCallbackWithContextLoad => _asyncCallbackWithContextLoad;

        private int _asyncForkLoad;
        public int AsyncForkLoad => _asyncForkLoad;

        private int _forkLoad;
        public int ForkLoad => _forkLoad;

        public double AQTime => (double)_asyncCallbackWithContextTime / _asyncTaskWithContextCount;

        private int _lastWorkerSpawnedTime;

        private int _asyncTaskWithContextTime;
        private int _asyncCallbackWithContextTime;
        private int _asyncTaskTime;
        
        private int _taskQueueCapacity;
        private readonly int _asyncFallbackCapacity; //TODO: Autogrow

        private int _asyncTaskWithContextCapacity;
        private readonly int _asyncTaskCapacity;//TODO: Autogrow

        private int _asyncCallbackWithContextCapacity;

        private readonly int _asyncForkCapacity;//TODO: Autogrow
        private readonly int _forkCapacity;//TODO: Autogrow

        private int _disposed;
        public bool Zeroed => _disposed > 0;

        private long _taskEnqueueCount;
        private long _taskDequeueCount;
        public long TaskDequeueCount => _taskDequeueCount;

        private long _asyncFallbackCount;
        public long AsyncFallbackCount => _asyncFallbackCount;

        
        private long _asyncTaskWithContextCount;
        public long AsyncTaskWithContextCount => _asyncTaskWithContextCount;

        private long _asyncTaskCount;
        public long AsyncTaskCount => _asyncTaskCount;
        
        private long _asyncCallbackWithContextCount;
        public long AsyncCallbackCount => _asyncCallbackWithContextCount;
        
        private long _asyncForkCount;
        public long AsyncForkCount => _asyncForkCount;
        public long ForkCount => _forkCount;
        private long _forkCount;
        
        private readonly TaskScheduler _fallbackScheduler;
        private readonly bool _native;


        //idle worker threads
        public int Idle => _taskQueue.WaitCount;

        public double Rate => _taskQueue.Cps(true);

        //waiting program threads
        public int Wait => _taskQueue.ReadyCount;
        public int ThreadCount => _taskQueueCapacity;
        public double LoadFactor => (double)Load / _taskQueueCapacity;
        public long Capacity => _taskQueueCapacity;

        public void GrowQueues()
        {
            //tasks
            for (var i = 0; i < _taskQueueCapacity; i++)
            {
                _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncSchedulerTask().FastPath().ConfigureAwait(false)
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
            }

            //forks with context
            for (var i = 0; i < _asyncFallbackCapacity; i++)
            {
                _ = Task.Factory.StartNew(static state =>((IoZeroScheduler)state).HandleAsyncFallback().FastPath().ConfigureAwait(false)
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
            }

            //async value callbacks with context
            for (var i = 0; i < _asyncTaskWithContextCapacity; i++)
            {
                _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncValueTaskWithContext().FastPath()
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);
            }

            //async value callbacks
            for (var i = 0; i < _asyncTaskCapacity; i++)
            {
                _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncValueTask().FastPath()
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);
            }

            //async callbacks
            for (var i = 0; i < _asyncCallbackWithContextCapacity; i++)
            {
                _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncCallback().FastPath()
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);
            }

            //async callbacks
            for (var i = 0; i < _asyncForkCapacity; i++)
            {
                _ = Task.Factory.StartNew(static async state => await ((IoZeroScheduler)state).ForkAsyncCallbacks().ConfigureAwait(false)
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);
            }

            //forks
            for (var i = 0; i < _forkCapacity; i++)
            {
                _ = Task.Factory.StartNew(static async state => await ((IoZeroScheduler)state).ForkCallbacks().FastPath()
                , this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);
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

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ZeroValueContinuation Prime(Func<object, ValueTask> valueFunc, object context)
            {
                Interlocked.Exchange(ref ValueFunc, valueFunc);
                Interlocked.Exchange(ref Context, context);
                return this;
            }
        }

        private void TrackInit()
        {
            if (Interlocked.Increment(ref _initCount) == InitExpected)
                Interlocked.Exchange(ref _initialized, 1);
        }

        private async ValueTask HandleAsyncSchedulerTask()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                var job = await _taskQueue.WaitAsync().FastPath().ConfigureAwait(false);
                try
                {
                    Interlocked.Increment(ref _taskQueueLoad);
                    if (!TryExecuteTask(job))
                    {
                        if (job.Status < TaskStatus.Running)
                            LogManager.GetCurrentClassLogger().Fatal($"{nameof(HandleAsyncSchedulerTask)}: Unable to execute task, id = {job.Id}, state = {job.Status}, async-state = {job.AsyncState}, success = {job.IsCompletedSuccessfully}");
                    }
                    else
                        Interlocked.Increment(ref _taskDequeueCount);
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Error(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _taskQueueLoad);
                    job.Dispose();
                }
            }
        }

        private async ValueTask HandleAsyncFallback()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                ZeroContinuation job = null;
                try
                {
                    job = await _asyncFallbackQueue.WaitAsync().FastPath();//TODO:fallback?
                    Interlocked.Increment(ref _asyncFallBackLoad);
                    job.Callback(job.State);
                    Interlocked.Increment(ref _asyncFallbackCount);
                }
                catch (TaskCanceledException) { }
                catch (OperationCanceledException) { }
                catch when (Zeroed) { }
                catch (InvalidOperationException){}
#if RELEASE
                catch (NullReferenceException) { }
#endif
                catch (Exception e) when (!Zeroed)
                {
//#if DEBUG
                    LogManager.GetCurrentClassLogger().Error(e);
//#endif
                }
                finally
                {
                    Interlocked.Decrement(ref _asyncFallBackLoad);
                    if (!Zeroed)
                    {
                        if (Interlocked.CompareExchange(ref _fbQQuickSlot, job, null) != null)
                            _callbackHeap.Return(job);
                    }
                }
            }
        }

        private async ValueTask HandleAsyncValueTaskWithContext()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                var job = await _asyncTaskWithContextQueue.WaitAsync().FastPath().ConfigureAwait(_native);

                try
                {
                    if ((double)_asyncTaskWithContextLoad / _asyncTaskWithContextCapacity > 0.84)
                    {
                        Interlocked.Increment(ref _asyncTaskWithContextCapacity);
                        _ = Task.Factory.StartNew(static async state =>
                            {
                                var @this= (IoZeroScheduler)state;
                                await @this.HandleAsyncValueTaskWithContext().FastPath();
                            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);
                    }
                    Interlocked.Increment(ref _asyncTaskWithContextLoad);
                    await job.ValueFunc(job.Context).FastPath();
                    Interlocked.Increment(ref _asyncTaskWithContextCount);
                    Interlocked.Add(ref _asyncTaskWithContextTime, job.Timestamp.ElapsedMs());
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _asyncTaskWithContextLoad);
                    if (!Zeroed)
                        _contextHeap.Return(job);
                }
            }
        }

        private async ValueTask HandleAsyncCallback()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                var job = await _asyncCallbackWithContextQueue.WaitAsync().FastPath().ConfigureAwait(_native);
                try
                {
                    Interlocked.Increment(ref _asyncCallbackWithContextLoad);
                    job.Callback(job.State);
                    Interlocked.Increment(ref _asyncCallbackWithContextCount);
                    Interlocked.Add(ref _asyncCallbackWithContextTime, job.Timestamp.ElapsedMs());
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Error(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _asyncCallbackWithContextLoad);
                    if (!Zeroed)
                        _callbackHeap.Return(job);
                }
            }
        }
        private async ValueTask HandleAsyncValueTask()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                var job = await _asyncTaskQueue.WaitAsync().FastPath().ConfigureAwait(_native);
                try
                {
                    Debug.Assert(TaskScheduler.Current.Id == IoZeroScheduler.ZeroDefault.Id);
                    Interlocked.Increment(ref _asyncTaskLoad);
                    await job.ValueTask.FastPath();
                    Interlocked.Increment(ref _asyncTaskCount);
                    Interlocked.Add(ref _asyncTaskTime, job.Timestamp.ElapsedMs());
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Error(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _asyncTaskLoad);
                    if (!Zeroed)
                        _contextHeap.Return(job);
                }
            }
        }

        private async ValueTask ForkAsyncCallbacks()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                try
                {
                    var job = await _asyncForkQueue.WaitAsync().FastPath().ConfigureAwait(false);
                    Interlocked.Increment(ref _asyncForkLoad);
                    await job().FastPath();
                    Interlocked.Increment(ref _asyncForkCount);
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _asyncForkLoad);
                }
            }
        }

        private async ValueTask ForkCallbacks()
        {
            TrackInit();
            while (!_asyncTasks.IsCancellationRequested)
            {
                var job = await _forkQueue.WaitAsync().FastPath();
                try
                {
                    Interlocked.Increment(ref _forkLoad);
                    job();
                    Interlocked.Increment(ref _forkCount);
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
                finally
                {
                    Interlocked.Decrement(ref _forkLoad);
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
            //while (_taskQueue.)
            //{
            //    l.Add(item);
            //}
            return l;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureInit()
        {
            if (_initialized != 0) return;

            var spinWait = new SpinWait();
            while(_initialized == 0 && !Zeroed)
                spinWait.SpinOnce();
        }

        public void Queue(Task task) => QueueTask(task);
        /// <summary>
        /// Queue this task to the scheduler
        /// </summary>
        /// <param name="task">The task</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void QueueTask(Task task)
        {
            if(task.IsCompletedSuccessfully)
                return;

            //schedule the task
            if (_taskQueue.Release(task, true) < 0)
                throw new InternalBufferOverflowException($"{nameof(_taskQueue)}: {_taskQueue.Description}");

            //insane checks
            if (Load > Environment.ProcessorCount && LoadFactor > WorkerSpawnThreshold && _lastWorkerSpawnedTime.ElapsedMs() > WorkerSpawnBurstTimeMs && _taskQueueCapacity < short.MaxValue / WorkerSpawnPassThrough || LoadFactor > 0.99)
            {
                //_lastWorkerSpawnedTime = Environment.TickCount;
                FallbackContext(static state =>
                {
                    var @this = (IoZeroScheduler)state;
                    if (Interlocked.Decrement(ref _workerSpawnBurstMax) > 0 || @this.LoadFactor > WorkerSpawnThreshold)
                    {
                        Interlocked.Increment(ref @this._taskQueueCapacity);
                        _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncSchedulerTask().ConfigureAwait(false)
                            , @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.HideScheduler, Default);

                        Interlocked.Increment(ref @this._asyncTaskWithContextCapacity);
                        _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncValueTaskWithContext()
                            , @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);

                        Interlocked.Increment(ref @this._asyncCallbackWithContextCapacity);
                        //async callbacks
                        _ = Task.Factory.StartNew(static state =>((IoZeroScheduler)state).HandleAsyncCallback()
                        , @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);

                        _ = Task.Factory.StartNew(static state => ((IoZeroScheduler)state).HandleAsyncFallback().ConfigureAwait(false)
                        , @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, ZeroDefault);

                        if (_workerSpawnBurstMax == 0)
                            Interlocked.Exchange(ref _workerSpawnBurstMax, WorkerSpawnBurstMax);

                        @this._lastWorkerSpawnedTime = Environment.TickCount;
                        Interlocked.MemoryBarrier();
                        Console.WriteLine($" - Adding zero thread {@this._taskQueueCapacity}, load = {@this.LoadFactor * 100:0.0}%");
                    }
                }, this);

                Thread.Yield();
            }

            Interlocked.Increment(ref _taskEnqueueCount);
        }

        /// <summary>Tries to execute the task synchronously on this scheduler.</summary>
        /// <param name="task">The task to execute.</param>
        /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued to the scheduler.</param>
        /// <returns>true if the task could be executed; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            //////TODO:code smell
            //return task.IsCompletedSuccessfully || TryExecuteTaskInlineOnTargetScheduler(task, _fallbackScheduler);

            return (taskWasPreviouslyQueued) ?
                    TryExecuteTaskInlineOnTargetScheduler(task, _fallbackScheduler) : TryExecuteTask(task);

            //if (taskWasPreviouslyQueued)
            //    return TryExecuteTaskInlineOnTargetScheduler(task, _fallbackScheduler);
            //else
            //{
            //    QueueTask(task);
            //}
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
                return t.Result;
            }
            catch
            {
                _ = t.Exception;
                throw;
            }
            finally { t.Dispose(); }
        }

        ///// <summary>
        ///// returns a diagnostic result back into the heap
        ///// </summary>
        ///// <param name="value"></param>
        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public void Return(List<int> value)
        //{
        //    if(!Zeroed) 
        //        _diagnosticsHeap.Return(value);
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool QueueCallback(Action<object> callback, object state)
        {
            ZeroContinuation handler = null;
            try
            {
                handler = _callbackHeap.Take(); 
                
                if (handler == null) return false;

                handler.Callback = callback;
                handler.State = state;
                return _asyncCallbackWithContextQueue.Release(handler, true) >= 0;
            }
            finally
            {
                if (handler != null)
                    _callbackHeap.Return(handler);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncContext<T>(Func<object,ValueTask> valueTask, T context) => _asyncTaskWithContextQueue.Release(_contextHeap.Take().Prime(valueTask, context), true) >= 0;
        
        //API
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(ValueTask task)
        {
            var c = _contextHeap.Take();
            c.ValueTask = task;
            return _asyncTaskQueue.Release(c, true) >= 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LoadAsyncCallback(Func<ValueTask> callback) => _asyncForkQueue.Release(callback, true) >= 0;

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Fork(Action callback) => _forkQueue.Release(callback, true) >= 0;

        /// <summary>
        /// Isolates the underlying scheduler API that this scheduler needs to function
        ///
        /// It needs to be able to schedule continuations async without interfering with this scheduler. 
        /// </summary>
        /// <param name="callback">The callback</param>
        /// <param name="context">The state</param>
        /// <returns>True on success, fail otherwise</returns>
        /// <exception cref="OutOfMemoryException"></exception>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool FallbackContext(Action<object> callback, object context = null)
        {
            var qItem = Interlocked.CompareExchange(ref _fbQQuickSlot, null, _fbQQuickSlot)??_callbackHeap.Take();
            if (qItem == null) throw new OutOfMemoryException($"{nameof(FallbackContext)}: {_callbackHeap.Description}");
            qItem.Callback = callback;
            qItem.State = context;
            qItem.Timestamp = Environment.TickCount;
            Interlocked.MemoryBarrier();

            try
            {
                return _asyncFallbackQueue.Release(qItem, true) > 0;
            }
            catch
            {
                // ignored
            }

            return false;
        }

        ~IoZeroScheduler()
        {
            Dispose(false);
        }

        private void Dispose(bool disposed)
        {
            if(Interlocked.CompareExchange(ref _disposed, 1, 0) == 1)
                return;

            if (disposed)
            {
                _asyncTasks.Cancel();

                _asyncForkQueue.ZeroSem();
                _asyncCallbackWithContextQueue.ZeroSem();

                _asyncTaskQueue.ZeroSem();
                _asyncTaskWithContextQueue.ZeroSem();
                _asyncFallbackQueue.ZeroSem();
                _taskQueue.ZeroSem();


                _callbackHeap.ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
                _contextHeap.ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
                //_diagnosticsHeap.ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
            }
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
