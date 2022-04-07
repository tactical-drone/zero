﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using NLog.Config;
using zero.core.misc;
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
            Zero = new IoZeroScheduler();
            ZeroDefault = Zero;
            //ZeroDefault = Default; //TODO: Uncomment to enable native .net scheduler...
        }
        public IoZeroScheduler(CancellationTokenSource asyncTasks = null)
        {
            _ = base.Id; // force ID creation of the default scheduler
            
            if(!Enabled)
                return;
            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Math.Max(Environment.ProcessorCount >> 1, 2);
            _queenCount = Math.Max(Environment.ProcessorCount >> 4, 1) + 1;
            var capacity = MaxWorker;

            _workQueue = new IoZeroQ<Task>(string.Empty, capacity * 2, true);
            _queenQueue = new IoZeroQ<ZeroSignal>(string.Empty, capacity,true);
            _signalHeap = new IoHeap<ZeroSignal>(string.Empty, capacity, (_, _) => new ZeroSignal(), true)
            {
                PopAction = (signal, _) =>
                {
                    signal.Processed = 0;
                    signal.Task = null;
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
                Volatile.Write(ref _pollWorker[i], MallocWorkTaskCore);
                spawnWorker($"zero scheduler worker thread {i}", i, _workQueue, null, ThreadPriority.Normal, WorkerHandler);
            }

            for (var i = 0; i < _queenCount; i++)
            {
                //spawn queen thread.
                Volatile.Write(ref _pollQueen[i], MallocQueenTaskCore);
                spawnQueen($"zero scheduler queen thread {i}", i, _queenQueue, null,
                    ThreadPriority.Highest, QueenHandler);
            }
        }

        internal class ZeroSignal
        {
            public volatile Task Task;
            public volatile int Processed = -1;
        }

        //The rate at which the scheduler will be allowed to "burst" allowing per tick unchecked new threads to be spawned until one of them spawns
        private static readonly int WorkerSpawnBurstTimeMs = 100;
        private static readonly int MaxWorker = (int)Math.Pow(2, Math.Max((Environment.ProcessorCount >> 1) + 1, 17));
        public static readonly TaskScheduler ZeroDefault;
        public static readonly IoZeroScheduler Zero;
        private readonly CancellationTokenSource _asyncTasks;
        private readonly IoZeroResetValueTaskSource<bool>[] _pollWorker;
        private readonly IoZeroResetValueTaskSource<bool>[] _pollQueen;
        private readonly int[] _workerPunchCards;
        private readonly int[] _queenPunchCards;
        private volatile int _dropWorker;
        private IoZeroQ<Task> _workQueue;
        private readonly IoZeroQ<ZeroSignal> _queenQueue;
        private readonly IoHeap<ZeroSignal> _signalHeap;
        private readonly IoHeap<List<int>> _diagnosticsHeap;

        private volatile int _workerCount;
        private volatile int _queenCount;
        private long _completedWorkItemCount;
        private long _completedQItemCount;
        private volatile int _lastSpawnedWorker = Environment.TickCount;

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
        public double LoadFactor => (double) Load / _workerCount;
        public double QLoadFactor => (double) QLoad / _queenCount;
        public long Capacity => _workQueue.Capacity;

        static IoZeroResetValueTaskSource<bool> MallocWorkTaskCore => new IoZeroResetValueTaskSource<bool>();
        static IoZeroResetValueTaskSource<bool> MallocQueenTaskCore => new IoZeroResetValueTaskSource<bool>();

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

            if (s.Processed > 0 || s.Task.Status > TaskStatus.WaitingToRun)
                return false;
            
            //poll a worker, or create a new one if none are available
            try
            {
                if (!ThreadPool.UnsafeQueueUserWorkItem(static state => 
                    {
                        try
                        {
                            var (@this, s, workerId) = (ValueTuple<IoZeroScheduler, ZeroSignal, int>)state;

                            if (s.Processed > 0)
                                return;

                            if (s.Task.Status > TaskStatus.WaitingToRun)
                            {
                                if (Interlocked.CompareExchange(ref s.Processed, 1, 0) == 0)
                                {
                                    s.Task = null;
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
                                            if (Volatile.Read(ref @this._workerPunchCards[i]) == 0 && w.Ready())
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
                                        Volatile.Write(ref @this._pollWorker[newWorkerId], MallocWorkTaskCore);
                                        @this.SpawnWorker<Task>($"zero scheduler worker thread {newWorkerId}", newWorkerId, @this._workQueue, s.Task, ThreadPriority.Normal, WorkerHandler);
                                        @this._lastSpawnedWorker = Environment.TickCount;
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

        /// <summary>
        /// Spawn a new worker thread
        /// </summary>
        /// <param name="desc">A Description</param>
        /// <param name="id">worker id</param>
        /// <param name="prime">task to prime this thread with</param>
        /// <param name="queue">The Q o use</param>
        /// <param name="priority">Thread priority</param>
        /// <param name="callback">work handler</param>
        private void SpawnWorker<T>(string desc, int id, IoZeroQ<T> queue, Task prime = null, ThreadPriority priority = ThreadPriority.Normal, Func<IoZeroScheduler, T, int, bool> callback = null) where T : class
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
                    var (@this, queue, desc, xId, prime, callback, priority) =
                        (ValueTuple<IoZeroScheduler, IoZeroQ<T>, string, int, Task, Func<IoZeroScheduler, T, int, bool>, ThreadPriority>)state;

                    bool isWorker = priority != ThreadPriority.Highest;
                    //fast prime task to unwind current worker dead lockers
                    if (prime is { Status: <= TaskStatus.WaitingToRun })
                        @this.TryExecuteTask(prime);
#if DEBUG
                    var jobsProcessed = 0;
#endif
                    var syncRoot = !isWorker
                        ? Volatile.Read(ref @this._pollQueen[xId])
                        : Volatile.Read(ref @this._pollWorker[xId]);

                    //process tasks
                    while (!@this._asyncTasks.IsCancellationRequested)
                    {
                        try
                        {
                            //wait on work q pressure
                            if (queue.Count == 0 && syncRoot.Ready(true))//TODO: Design flaw
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

                            try
                            {
                                int ramp;
                                //Service the Q
                                if (!isWorker)
                                {
                                    ramp = 0;
                                    Interlocked.Exchange(ref @this._queenPunchCards[xId], 1);
#if __TRACE__
                                    Console.WriteLine($"[[QUEEN]] CONSUMING FROM Q {workerId},  Q = {queue.Count}/{@this.Active.Count()} - total jobs process = {@this.CompletedWorkItemCount}");
#endif
                                }
                                else
                                {
                                    ramp = 3;
                                    Interlocked.Exchange(ref @this._workerPunchCards[xId], 1);
                                }

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
                                        else if(isWorker && work == null)
                                            await Task.Yield();
                                    }
                                    catch (Exception e)
                                    {
#if DEBUG
                                        LogManager.GetCurrentClassLogger().Error(e,
                                            $"{nameof(IoZeroScheduler)}: wId = {xId}/{@this._workerCount}, this = {@this}, wq = {@this?._workQueue}, work = {work}, q = {syncRoot}");
#endif
                                    }
                                    finally
                                    {
                                        work = null;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);

                                try
                                {
                                    Console.WriteLine($"---> status = {syncRoot.GetStatus((short)syncRoot.Version)}");
                                }
                                catch
                                {
                                    // ignored
                                }
                            }
                            finally
                            {
                                if (isWorker)
                                {
                                    Interlocked.Exchange(ref @this._workerPunchCards[xId], 0);
                                }
                                else
                                {
                                    Interlocked.Exchange(ref @this._queenPunchCards[xId], 0);
                                }
                            }
                        }
                        catch (Exception e)
                        {
#if DEBUG
                            LogManager.GetCurrentClassLogger().Error(e,
                                $"{nameof(IoZeroScheduler)}: wId = {xId}/{@this._workerCount}, this = {@this != null}, wq = {@this?._workQueue}, q = {syncRoot != null}");
#endif
                        }
                        

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

                        //Oneshot workers end here.
                        if (priority == ThreadPriority.AboveNormal)
                        {
                            break;
                        }
                    }

                    if (isWorker)
                    {
                        Console.WriteLine($"KILLING WORKER THREAD id ={xId} - has = {desc})");
                        Volatile.Write(ref @this._workerPunchCards[xId], -1);

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
                catch
                {
                    // ignored
                }
            }

            var t = new Thread(ThreadWorker)
            {
                IsBackground = true,
                Name = $"{desc}[{id}]"
            };

            t.Start((this, queue, desc, id, prime, callback, priority));

            if (priority == ThreadPriority.Highest)
                Volatile.Write(ref _queenPunchCards[id], 0);
            else
                Volatile.Write(ref _workerPunchCards[id], 0);
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
            if (task.CreationOptions.HasFlag(TaskCreationOptions.LongRunning))
            {
                new Thread(state =>
                {
                    var (@this, t) = (ValueTuple<IoZeroScheduler, Task>)state;
                    @this.TryExecuteTask(t);
                })
                {
                    IsBackground = true,
                    Name = ".Zero LongRunning Thread"
                }.Start((this, task));
                return;
            }

            //queue the work for processing
            if (_workQueue.TryEnqueue(task) != -1)
                PollQueen(task);
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

            ZeroSignal zeroSignal = null;
            ZeroSignal tmpSignal;
            var polled = false;

            try
            {
                zeroSignal = _signalHeap.Take();
                Debug.Assert(zeroSignal != null);
                Debug.Assert(task != null);
                //prepare work queen poll signal
                
                zeroSignal.Task = task;

                if (task.Status <= TaskStatus.WaitingToRun)
                {
                    if (_queenQueue.TryEnqueue(tmpSignal = zeroSignal) != -1)
                    {
                        zeroSignal = null;
                    }
                    else
                    {
                        Console.WriteLine($"Failed to EQ: {_queenQueue.Description}");
                    }
                }
                else
                    return false;
            }
            finally
            {
                if(zeroSignal != null)
                    _signalHeap.Return(zeroSignal);
            }

            if (zeroSignal != null)
                return false;
            
            //poll a queen that there is work to be done
            var qId = _queenCount;
            while (!polled && task.Status <= TaskStatus.WaitingToRun && qId-- > 0)
            {
                if (tmpSignal.Processed > 0 || Volatile.Read(ref _queenPunchCards[qId]) == 1)
                {
                    polled = false;
                    break;
                }
                
                var q = _pollQueen[qId];
                
                if (tmpSignal.Processed == 0 && Volatile.Read(ref _queenPunchCards[qId]) == 0)
                {
                    if (q.Ready())//TODO: Design flaw
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
            if (taskWasPreviouslyQueued && !Thread.CurrentThread.IsThreadPoolThread)
                return false;

            return (!taskWasPreviouslyQueued) ?
                TryExecuteTask(task) :
                TryExecuteTaskInlineOnTargetScheduler(task);
        }

        /// <summary>
        /// Implements a reasonable approximation for TryExecuteTaskInline on the underlying scheduler,
        /// which we can't call directly on the underlying scheduler.
        /// </summary>
        /// <param name="task">The task to execute inline if possible.</param>
        /// <returns>true if the task was inlined successfully; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryExecuteTaskInlineOnTargetScheduler(Task task)
        {
            var t = new Task<bool>(static s =>
            {
                var tuple = (ValueTuple<IoZeroScheduler, Task>)s!;
                return tuple.Item1.TryExecuteTask(tuple.Item2);
            }, (this, task));
            try
            {
                t.RunSynchronously(Default);
                return t.Result;
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
    }
}
