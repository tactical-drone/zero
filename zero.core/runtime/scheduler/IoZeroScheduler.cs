﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Schema;
using Microsoft.VisualStudio.Threading;
using NLog;
using NLog.Config;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

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
            _workerCount = Math.Max(Environment.ProcessorCount >> 1, 2);
            _queenCount = Math.Max(Environment.ProcessorCount >> 4, 1) + 1;
            _asyncCount = _workerCount>>1;
            var capacity = MaxWorker + 1;

            //TODO: tuning
            _workQueue = new IoZeroQ<Task>(string.Empty, capacity * 2, true);
            _asyncQueue = new IoZeroQ<Func<ValueTask>>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel:MaxWorker - 1, zeroAsyncMode: true);
            _syncQueue = new IoZeroQ<ZeroContinuation>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: false);
            _oneShotQueue = new IoZeroQ<Action>(string.Empty, (MaxWorker + 1) * 2, true, _asyncTasks, concurrencyLevel: MaxWorker - 1, zeroAsyncMode: true);

            _queenQueue = new IoZeroQ<ZeroSignal>(string.Empty, capacity,true);
            _signalHeap = new IoHeap<ZeroSignal>(string.Empty, capacity, (_, _) => new ZeroSignal(), true)
            {
                PopAction = (signal, _) =>
                {
                    signal.Processed = 0;
                    signal.Task = null;
                }
            };

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

            _pollWorker = new IoZeroResetValueTaskSource<bool>[MaxWorker];
            _pollQueen = new IoZeroResetValueTaskSource<bool>[MaxWorker];
            _workerPunchCards = new int[MaxWorker];
            Array.Fill(_workerPunchCards, -1);
            _queenPunchCards = new int[MaxWorker];
            Array.Fill(_queenPunchCards, -1);

            //These two functions seem to be blending state after continuations, separating them manually might help?
            var spawnQueen = SpawnWorker<ZeroSignal>;
            var spawnWorker = SpawnWorker<Task>;

            for (var i = 0; i < _workerCount; i++)
            {
                //spawn worker thread
                _pollWorker[i] = MallocWorkTaskCore;
                spawnWorker($"zero scheduler worker thread {i}", i, _workQueue, null, IoMark.Worker, WorkerHandler);
            }

            for (var i = 0; i < _queenCount; i++)
            {
                //spawn queen thread.
                _pollQueen[i] = MallocQueenTaskCore;
                spawnQueen($"zero scheduler queen thread {i}", i, _queenQueue, null, IoMark.Queen, QueenHandler);
            }

            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this,i) = (ValueTuple<IoZeroScheduler,int>)state;
                    await @this.SpawnNoAsync(i).FastPath();
                },(this,i), CancellationToken.None,TaskCreationOptions.DenyChildAttach, _fallbackScheduler);
            }
            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.SpawnAsync(i).FastPath().ConfigureAwait(false);
                }, (this, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.HideScheduler, Default);
            }

            for (var i = 0; i < _asyncCount; i++)
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                    await @this.SpawnOneShotAsync(i).FastPath().ConfigureAwait(false);
                }, (this, i), CancellationToken.None, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.HideScheduler, Default);
            }
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
            Queen = 1 << 1,
            Async = 1 << 2
        }

        //The rate at which the scheduler will be allowed to "burst" allowing per tick unchecked new threads to be spawned until one of them spawns
        private static readonly int WorkerSpawnBurstTimeMs = 100;
        private static readonly int MaxWorker = short.MaxValue>>1;
        public static readonly TaskScheduler ZeroDefault;       
        public static readonly IoZeroScheduler Zero;
        public static readonly JoinableTaskFactory AsyncBridge = new JoinableTaskFactory(new JoinableTaskContext());
        private readonly CancellationTokenSource _asyncTasks;        
        private readonly IoZeroResetValueTaskSource<bool>[] _pollWorker;
        private readonly IoZeroResetValueTaskSource<bool>[] _pollQueen;
        private readonly int[] _workerPunchCards;
        private readonly int[] _queenPunchCards;
        private volatile int _dropWorker;
        private readonly IoZeroQ<Task> _workQueue;
        private readonly IoZeroQ<Func<ValueTask>> _asyncQueue;
        private readonly IoZeroQ<Action> _oneShotQueue;
        private readonly IoZeroQ<ZeroContinuation> _syncQueue;
        private readonly IoZeroQ<ZeroSignal> _queenQueue;
        
        private readonly IoHeap<ZeroContinuation> _callbackHeap;
        private readonly IoHeap<ZeroSignal> _signalHeap;
        private readonly IoHeap<List<int>> _diagnosticsHeap;

        private volatile int _workerCount;
        private volatile int _queenCount;
        private volatile int _asyncCount;
        private long _completedWorkItemCount;
        private long _completedQItemCount;
        private long _completedAsyncCount;
        private long _completedSyncCount;
        private volatile int _lastSpawnedWorker = Environment.TickCount;
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
                    if (_workerPunchCards[i] != 0)
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

        public int WLength => _workQueue?.Count??0;

        public int QLength => _queenQueue?.Count??0;
        public int ThreadCount => _workerCount;
        public int QThreadCount => _queenCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public long CompletedQItemCount => _completedWorkItemCount;

        public long CompletedAsyncCount => _completedAsyncCount;
        public double LoadFactor => (double) Load / _workerCount;
        public double QLoadFactor => (double) QLoad / _queenCount;
        public long Capacity => _workQueue.Capacity;

        static IoZeroResetValueTaskSource<bool> MallocWorkTaskCore => new IoZeroResetValueTaskSource<bool>(false);
        static IoZeroResetValueTaskSource<bool> MallocQueenTaskCore => new IoZeroResetValueTaskSource<bool>(false);

        /// <summary>
        /// Queens wake worker threads if there is work to be done.
        ///
        /// It starts its way at the most resent worker created working its way down.
        /// </summary>
        /// <param name="this">The scheduler</param>
        /// <param name="s">The signal to be sent</param>
        /// <param name="id">The queen id</param>
        /// <returns>True if successful, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool QueenHandler(IoZeroScheduler @this, ZeroSignal s, int id)
        {

#if _TRACE_
            var ts = Environment.TickCount;
            Console.WriteLine($"Queen ASYNC handler... POLLING WORKER..."); 
#endif

            //TODO: why is s sometimes null?
            if (s == null || s.Processed > 0 || s.Task.Status > TaskStatus.WaitingToRun)
                return false;
            
            //poll a worker, or create a new one if none are available
            try
            {
                if (!ThreadPool.UnsafeQueueUserWorkItem(static state => 
                    {
                        try
                        {
                            var (@this, s, workerId) = (ValueTuple<IoZeroScheduler, ZeroSignal, int>)state;

                            if (s.Processed != 0)
                                return;

                            if (s.Task.Status > TaskStatus.WaitingToRun)
                            {
                                if (Interlocked.CompareExchange(ref s.Processed, 1, 0) == 0)
                                {
                                    s.Task = null;
                                    s.Processed = -1;
                                    @this._signalHeap.Return(s);
                                }

                                return;
                            }

                            var blocked = @this.Blocked;
                            try
                            {
                                if (blocked.Count < @this._workerCount)
                                {
                                    var ramp = 2;
                                    for (var i = @this._workerCount; i-- > 0;)
                                    {
                                        try
                                        {
                                            var w = @this._pollWorker[i];
                                            if (@this._workerPunchCards[i] == 0 && w.Blocking)
                                            {
                                                w.SetResult(true);
#if _TRACE_
                                            Console.WriteLine($"Polled worker {i} from queen {workerId}, for task {s.Task!.Id}");
#endif
                                                if(ramp --> 0)
                                                    return;
                                            }
                                        }
                                        catch
                                        {
                                            // ignored
                                        }
                                    }
                                }

                                //mark this signal as processed
                                if (Interlocked.CompareExchange(ref s.Processed, 1, 0) == 0)
                                {
                                    s.Task = null;
                                    s.Processed = -1;
                                    @this._signalHeap.Return(s);
                                }

                                //spawn more workers, the ones we have are deadlocked
                                if (blocked.Count >= @this._workerCount && @this._lastSpawnedWorker.ElapsedMs() > WorkerSpawnBurstTimeMs)
                                {
                                    var newWorkerId = Interlocked.Increment(ref @this._workerCount) - 1;
                                    if (newWorkerId < MaxWorker)
                                    {
#if __TRACE__
                                    Console.WriteLine($"spawning more workers q[{newWorkerId}] = {@this._workQueue.Count}, l = {@this.Load}, {@this.Free.Count()}/{@this.Blocked.Count()}/{@this.Active.Count()}");
#endif
                                        @this._pollWorker[newWorkerId] = MallocWorkTaskCore;
                                        @this.SpawnWorker<Task>($"zero scheduler worker thread {newWorkerId}", newWorkerId, @this._workQueue, s.Task, IoMark.Worker, WorkerHandler);
                                        @this._lastSpawnedWorker = Environment.TickCount;

                                        _ = Task.Factory.StartNew(static async state =>
                                        {
                                            var (@this, i) = (ValueTuple<IoZeroScheduler, int>)state;
                                            await @this.SpawnAsync(i).FastPath();
                                        }, (@this, Interlocked.Increment(ref @this._asyncCount) - 1), CancellationToken.None, TaskCreationOptions.DenyChildAttach, @this._fallbackScheduler);

                                        //_ = Task.Factory.StartNew(static async state =>
                                        //{
                                        //    var @this = (IoZeroScheduler)state;
                                        //    await @this.SpawnNoAsync().FastPath();
                                        //}, @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
                                    }
                                    else
                                    {
                                        Interlocked.Decrement(ref @this._workerCount);
                                    }
                                }
                            }
                            finally
                            {
                                @this._diagnosticsHeap.Return(blocked);
                                if (Interlocked.CompareExchange(ref s.Processed, 1, 0) == 0)
                                {
                                    s.Task = null;
                                    s.Processed = -1;
                                    @this._signalHeap.Return(s);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }, (@this, s, id)))
                {
#if TRACE
                    Console.WriteLine($"Queen[{id}]: Unable to Q signal for task {s.Task.Id}, {ts.ElapsedMs()}ms, OOM");
#endif
                    return false;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

#if TRACE
            var d = ts.ElapsedMs();
            if(d > 1)
                Console.WriteLine($"QUEEN[{id}] SLOW => {d}ms, q length = {@this.QLength}");   
#endif
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool WorkerHandler(IoZeroScheduler ioZeroScheduler, Task task, int id)
        {
            var s = task.Status == TaskStatus.WaitingToRun && ioZeroScheduler.TryExecuteTask(task);
#if __TRACE__
            if(s)
                Console.WriteLine($"--> Processed task id = {task.Id}, q = {ioZeroScheduler.WLength}, good scheduler = {Current == ioZeroScheduler}");
            else
                Console.WriteLine($"X--> Processed task id = {task.Id}, {task.Status}");
#endif
            return s;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool AsyncHandler(IoZeroScheduler ioZeroScheduler, ZeroContinuation handler, int id)
        {
            try
            {
                handler.Callback(handler.State);
                return true;
            }
            finally
            {
                ioZeroScheduler._callbackHeap.Return(handler);
            }
        }

        /// <summary>
        /// Spawn a new worker thread
        /// </summary>
        /// <param name="desc">A Description</param>
        /// <param name="id">worker id</param>
        /// <param name="prime">task to prime this thread with</param>
        /// <param name="queue">The Q o use</param>
        /// <param name="priority">Thread priority</param>
        /// <param name="callback">work handler</param>
        private void SpawnWorker<T>(string desc, int id, IoZeroQ<T> queue, Task prime = null, IoMark mark = IoMark.Undefined, Func<IoZeroScheduler, T, int, bool> callback = null) where T : class
        {
            //Thread.CurrentThread.Priority = priority;
#if DEBUG
#if __TRACE__
var d = 0;
            if (Active.Any())
            {
                d = ((int)Active.Average()).ElapsedMs();
            }

            Console.WriteLine("-----------------------------------------------------------------------------------------------------------------------------------------");
            Console.WriteLine($"spawning {desc}[{id}], t = {ThreadCount}, l = {Load}/{Environment.ProcessorCount}({(double)Load / Environment.ProcessorCount * 100.0:0.0}%) ({LoadFactor * 100:0.0}%), q = {WLength}, z = {d}ms, a = {Active.Count()}, running = {Blocked.Count()}, Free = {Free.Count()}, ({(double)Blocked.Count() / Active.Count() * 100:0.0}%)");
#endif

#endif
            // ReSharper disable once AsyncVoidLambda
            static async void ThreadWorker(object state)
            {
                try
                {
                    var (@this, queue, desc, xId, prime, callback, mark) =
                        (ValueTuple<IoZeroScheduler, IoZeroQ<T>, string, int, Task, Func<IoZeroScheduler, T, int, bool>, IoMark>)state;

                    bool isWorker = mark == IoMark.Worker;
                    //fast prime task to unwind current worker dead lockers
                    if (prime is { Status: <= TaskStatus.WaitingToRun })
                        @this.TryExecuteTask(prime);
#if DEBUG
                    var jobsProcessed = 0;
#endif
                    var syncRoot = !isWorker ? @this._pollQueen[xId] : @this._pollWorker[xId];

                    //process tasks
                    while (!@this._asyncTasks.IsCancellationRequested)
                    {
                        try
                        {
                            //wait on work q pressure
                            if (queue.Count == 0 && !syncRoot.Blocking)//TODO: Design flaw
                            {
#if _TRACE_
                                if (priority == ThreadPriority.Highest)
                                {
                                    Console.WriteLine($"Worker {workerId} blocking... version = {(short)taskCore.Version}");
                                }
                                else
                                {
                                    Console.WriteLine($"Worker {workerId} blocking... version = {(short)taskCore.Version}");
                                }

#endif
                                try
                                {
                                    if (!isWorker)
                                    {
                                        if (!await syncRoot.WaitAsync().FastPath().ConfigureAwait(false))
                                        {
#if _TRACE_

                                            try
                                            {
                                                Console.WriteLine($"Queen {xId} unblocking... FAILED!");
                                                Console.WriteLine($"Queen {xId} unblocking... FAILED! status = {syncRoot.GetStatus((short)syncRoot.Version)}");
                                            }
                                            catch
                                            {
                                                // ignored
                                            }
#endif
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        if (!await syncRoot.WaitAsync().FastPath())
                                        {
#if _TRACE_
                                            try
                                            {
                                                Console.WriteLine($"Worker {xId} unblocking... FAILED!");
                                                Console.WriteLine($"Worker {xId} unblocking... FAILED! version = {syncRoot.GetStatus((short)syncRoot.Version)}");
                                            }
                                            catch
                                            {
                                                // ignored
                                            }
#endif
                                            continue;
                                        }
                                        //Thread.CurrentThread.Priority = ThreadPriority.Normal;
                                    }
                                }
                                catch
                                {
                                    continue;
                                }
#if _TRACE_
                                if (priority == ThreadPriority.Highest)
                                {
                                    //Console.WriteLine($"Queen {workerId} unblocking... ready = {queue.Count} version = {(short)taskCore.Version}");
                                }
                                else
                                {
                                    Console.WriteLine($"Worker {workerId} unblocking... ready = {queue.Count} version = {(short)taskCore.Version}");
                                }
#endif
                            }

#if _TRACE_
                            if(queue.Count > 0)
                                Console.WriteLine($"{desc}[{workerId}]: Q size = {queue.Count}, priority = {priority}, {jobsProcessed}");
#endif
                            int ramp;
                            //Service the Q
                            if (!isWorker)
                            {
                                ramp = 0;
                                if (Interlocked.CompareExchange(ref @this._queenPunchCards[xId], 1, 0) != 0)
                                    continue;

#if __TRACE__
                                    Console.WriteLine($"[[QUEEN]] CONSUMING FROM Q {workerId},  Q = {queue.Count}/{@this.Active.Count()} - total jobs process = {@this.CompletedWorkItemCount}");
#endif
                            }
                            else
                            {
                                ramp = 10;
                                if (Interlocked.CompareExchange(ref @this._workerPunchCards[xId], 1, 0) != 0)
                                    continue;
                            }

                            try
                            {
                                while (queue.TryDequeue(out var work) || ramp --> 0)
                                {
                                    try
                                    {
                                        if (work != null && callback(@this, work, xId))
                                        {
                                            if (!isWorker)
                                                Interlocked.Increment(ref @this._completedQItemCount);
                                            else
                                                Interlocked.Increment(ref @this._completedWorkItemCount);
#if DEBUG
                                            jobsProcessed++;
#endif
                                        }
                                        else if (isWorker && work == null)
                                        {
                                            //TODO: tuning
                                            //await Task.Yield();
                                        }
                                    }
//#if DEBUG
                                    catch (Exception e)
                                    {

                                        LogManager.GetCurrentClassLogger().Error(e,
                                            $"{nameof(IoZeroScheduler)}: wId = {xId}/{@this._workerCount}, this = {@this}, wq = {@this?._workQueue}, work = {work}, q = {syncRoot}");
                                    }
//#endif
                                    finally
                                    {
                                        work = null;
                                    }
                                }
                            }
                            catch (Exception e)
                            { 
                                Console.WriteLine(e);
                            }
                            finally
                            {
                                if (isWorker)
                                    Interlocked.Exchange(ref @this._workerPunchCards[xId], 0);
                                else
                                    Interlocked.Exchange(ref @this._queenPunchCards[xId], 0);
                            }
                        }
#if DEBUG
                        catch (Exception e)
                        {

                            LogManager.GetCurrentClassLogger().Error(e,
                                $"{nameof(IoZeroScheduler)}: wId = {xId}/{@this._workerCount}, this = {@this != null}, wq = {@this?._workQueue}, q = {syncRoot != null}");

                        }
#else
                        catch
                        {
                            // ignored
                        }
#endif


                        //drop zombie workers
                        if (@this._dropWorker > 0 && isWorker && xId == @this._workerCount - 1)
                        {
                            if (Interlocked.Decrement(ref @this._dropWorker) >= 0)
                            {
#if !__TRACE__
                                Console.WriteLine(
                                    $"!!!! KILLED WORKER THREAD ~> {desc}ZW[{xId}], processed = {@this.CompletedWorkItemCount}");
#endif
                                break;
                            }

                            Interlocked.Increment(ref @this._dropWorker);
                        }
                    }

                    if (isWorker)
                    {
                        Console.WriteLine($"KILLING WORKER THREAD id ={xId} - has = {desc})");
                        @this._workerPunchCards[xId] = -1;

                        var wTmp = @this._pollWorker[xId];
                        @this._pollWorker[xId] = null;

                        try
                        {
                            wTmp.SetException(new ThreadInterruptedException($"Inactive thread workerId = {xId} was purged"));
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    // ignored
                }
            }

            var t = new Thread(ThreadWorker)
            {
                IsBackground = true,
                Name = $"{desc}[{id}]"
            };

            t.Start((this, queue, desc, id, prime, callback, mark));

            if (mark.HasFlag(IoMark.Queen))
                _queenPunchCards[id] = 0;
            else
                _workerPunchCards[id] = 0;
        }

        private async ValueTask SpawnAsync(int threadIndex)
        {
            await foreach (var job in _asyncQueue.PumpOnConsumeAsync(threadIndex))
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

        private async ValueTask SpawnOneShotAsync(int threadIndex)
        {
            await foreach (var job in _oneShotQueue.PumpOnConsumeAsync(threadIndex))
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

        private async ValueTask SpawnNoAsync(int threadIndex)
        {
            await foreach (var job in _syncQueue.BalanceOnConsumeAsync(threadIndex).ConfigureAwait(false))
            {
                try
                {
                    job.Callback(job.State);
                    Interlocked.Increment(ref _completedSyncCount);
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Trace(e);
                }
            }
        }

        /// <summary>
        /// Gets a list of tasks held by this scheduler
        /// </summary>
        /// <returns></returns>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _workQueue;
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
            if (_workQueue.TryEnqueue(task) != -1)
            {
                Thread.Yield(); //TODO: Tuning
                PollQueen(task);
            }
            else
            {
                throw new InternalBufferOverflowException($"{nameof(_workQueue)}: count = {_workQueue.Count}, capacity {_workQueue.Capacity}");
            }
        }

        /// <summary>
        /// polls a queen
        /// </summary>
        /// <param name="task">A task associated with this poll</param>
        /// <returns>True if a poll was sent, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool PollQueen(Task task)
        {
            Debug.Assert(task != null);

            //Fast path
            if (task.Status > TaskStatus.WaitingToRun)
                return false;

            ZeroSignal zeroSignal = null;
            var polled = false;
            var queued = false;
            try
            {
                zeroSignal = _signalHeap.Take();
                Debug.Assert(zeroSignal != null);
                Debug.Assert(task != null);

                zeroSignal.Task = task;
                if (task.Status == TaskStatus.WaitingToRun && _queenQueue.TryEnqueue(zeroSignal) != -1)
                    queued = true;
            }
            finally
            {
                if(!queued && zeroSignal != null)
                    _signalHeap.Return(zeroSignal);
            }

            if (!queued)
                return false;

            //poll a queen that there is work to be done
            var qId = _queenCount;
            while (!polled && task.Status <= TaskStatus.WaitingToRun && zeroSignal.Processed == 0 && qId-- > 0)
            {
                var q = _pollQueen[qId];
                if (_queenPunchCards[qId] == 0)
                {
                    if (q.Blocking)//TODO: Design flaw
                    {
                        try
                        {
                            q.SetResult(true);
#if _TRACE_
                        Console.WriteLine($"load = {QLoad}, Polled queen {qId} for task id {task.Id}");
#endif
                            polled = true;
                            break;
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }
            }

#if __TRACE__
            if (!polled && task.Status <= TaskStatus.WaitingToRun)
                Console.WriteLine($"Unable to poll any of the {_queenCount} queens. backlog = {_queenQueue.Count}, Load = {QLoad} !!!!!!!!");
#endif
            
            return polled;
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
            foreach (var task in Zero._workQueue)
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
                result = _syncQueue.TryEnqueue(handler);
                return result > 0;
            }
            finally
            {
                if(result <= 0 && handler != null)
                    _callbackHeap.Return(handler);
            }
        }

        //API
        public void TryExecuteTaskInline(Task task) => TryExecuteTaskInline(task, false);
        public void Queue(Task task) => QueueTask(task);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool QueueSyncCallback(Func<ValueTask> callback, object state = null) => _asyncQueue.TryEnqueue(callback) > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool QueueOneShot(Action callback, object state = null) => _oneShotQueue.TryEnqueue(callback) > 0;
    }
}
