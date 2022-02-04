using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using NLog.LayoutRenderers;
using zero.core.misc;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
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
            Zero = new IoZeroScheduler();
            ZeroDefault = Zero;

            //ZeroDefault = Default; 
        }
        public IoZeroScheduler(CancellationTokenSource asyncTasks = null)
        {
            _ = base.Id; // force ID creation of the default scheduler
            
            if(!Enabled)
                return;
            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Math.Max(Environment.ProcessorCount >> 1, 2);
            var capacity = MaxWorker;

            Volatile.Write(ref _workQueue, new IoZeroQ<Task>(string.Empty, capacity, true));
            Volatile.Write(ref _queenQueue, new IoZeroQ<ZeroSignal>(string.Empty, capacity, true));
            Volatile.Write(ref _signalHeap, new IoHeap<ZeroSignal>(string.Empty, capacity, true)
            {
                Malloc = (_, _) => new ZeroSignal(),
                PopAction = (signal, _) =>
                {
                    signal.Processed = 0;
                }
            });
            Volatile.Write(ref _pollWorker, new IoManualResetValueTaskSource<bool>[MaxWorker]);
            Volatile.Write(ref _pollQueen, new IoManualResetValueTaskSource<bool>[MaxWorker]);
            Volatile.Write(ref _workerPunchCards, new int[MaxWorker]);
            Array.Fill(_workerPunchCards, -1);
            Volatile.Write(ref _queenPunchCards, new int[MaxWorker]);
            Array.Fill(_queenPunchCards, -1);

            var spawnQueen = SpawnWorker<ZeroSignal>;
            var spawnWorker = SpawnWorker<Task>;

            for (var i = 0; i < _workerCount; i++)
            {
                //spawn worker thread
                Volatile.Write(ref _pollWorker[i], MallocWorkTaskCore);
                spawnWorker($"zero scheduler worker thread {i}", i, Volatile.Read(ref _workQueue), null, ThreadPriority.Normal, WorkerHandler);

                //spawn queen thread.
                Volatile.Write(ref _pollQueen[i], MallocQueenTaskCore);
                spawnQueen($"zero scheduler queen thread {i}", i, Volatile.Read(ref _queenQueue), null, ThreadPriority.Highest, QueenHandler);
            }
            Interlocked.Exchange(ref _queenCount, _workerCount);

            //new Thread(
            Task.Factory.StartNew(static state =>
            {
                var @this = (IoZeroScheduler)state;
                while (!@this._asyncTasks.IsCancellationRequested)
                {
                    try
                    {
                        var fuckingValue = @this._lastWorkerDispatched.ElapsedMs();
                        var omfg1 = @this._workQueue.Count > 0;
                        var omfg2 = fuckingValue > WorkerSpawnRate;
                        var omfg = omfg1 && omfg2;
                        if ( /*@this.Blocked.Count() == _workerCount &&*/ omfg)
                        {
                            //int newWorkerId;

                            //if ((newWorkerId = Interlocked.Increment(ref _workerCount) - 1) < MaxWorker)
                            {
                                //Console.WriteLine($"pre-added zero scheduler (one-shot) worker thread {newWorkerId}, q = {@this._workQueue.Count}");
                                //Volatile.Write(ref @this._pollWorker[newWorkerId], MallocWorkTaskCore);
                                //@this.SpawnWorker($"zero scheduler (one-shot) worker thread {newWorkerId}", newWorkerId, queue: Volatile.Read(ref @this._workQueue), callback: WorkerHandler, priority: ThreadPriority.AboveNormal);
                                _ = Task.Factory.StartNew(static state =>
                                {
                                    var @this = (IoZeroScheduler)state;
                                    if (@this._workQueue.TryPeek(out var head))
                                    {
                                        var w = @this.Active.ToList().FindIndex(t => t == 0);
                                        if (w != -1)
                                        {
                                            try
                                            {
                                                @this._pollWorker[w].SetResult(true);
                                            }
                                            catch
                                            {

                                            }
                                        }
                                        else if (@this.PollQueen(head))
                                        {
                                            Console.WriteLine(
                                                $"added zero scheduler (one-shot) worker thread task id = {head.Id} , status = {head.Status}");
                                        }
                                    }
                                }, @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
                            }
                            //else
                            //{ 
                            //    Interlocked.Decrement(ref _workerCount);
                            //}
                        }

                        Thread.Sleep(WorkerSpawnRate);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
            //{
            //    IsBackground = true
            //}.Start(this);
        }

        internal class ZeroSignal
        {
            public volatile Task Task;
            public volatile int Processed;
        }

        private static readonly int SlowWorkerThreshold = 5000;
        private static readonly int WorkerSpawnRate = 16*100;
        private static readonly int WorkerExpireThreshold = 10;
        private static readonly int MaxLoad = Environment.ProcessorCount * 10;

        private static readonly int MaxWorker = (int)Math.Pow(2, 16);
        public static readonly TaskScheduler ZeroDefault;
        public static readonly IoZeroScheduler Zero;
        private CancellationTokenSource _asyncTasks;
        private volatile IoManualResetValueTaskSource<bool>[] _pollWorker;
        private volatile IoManualResetValueTaskSource<bool>[] _pollQueen;
        private volatile int[] _workerPunchCards;
        private volatile int[] _queenPunchCards;
        private volatile int _dropWorker;
        private volatile IoZeroQ<Task> _workQueue;
        private volatile IoZeroQ<ZeroSignal> _queenQueue;
        private volatile IoHeap<ZeroSignal> _signalHeap;
        
        private volatile int _workerCount;
        private volatile int _queenCount;
        private volatile int _lastWorkerDispatched;
        private int _lastQueenDispatched;
        //private int _load;
        //private int _qload;
        private long _completedWorkItemCount;
        private long _completedQItemCount;
        private volatile int _lastSpawnedWorker;

        public IEnumerable<int> Active => _workerPunchCards?.Take(_workerCount).Where(t => t >= 0);
        public IEnumerable<int> Blocked => Active?.Where(t => t != 0);
        public IEnumerable<int> Free => Active?.Where(t => t == 0);

        public IEnumerable<int> QActive => _queenPunchCards?.Take(_queenCount).Where(t => t >= 0);
        public IEnumerable<int> QBlocked => QActive?.Where(t => t != 0);
        public IEnumerable<int> QFree => QActive?.Where(t => t == 0);

        public int Load => Blocked?.Count()??0;
        public int QLoad => QBlocked?.Count()??0;
        public int WLength => _workQueue?.Count??0;

        public int QLength => _queenQueue?.Count??0;
        public int ThreadCount => _workerCount;
        public int QThreadCount => _queenCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public long CompletedQItemCount => _completedWorkItemCount;
        public double LoadFactor => (double) Load / _workerCount;
        public double QLoadFactor => (double) QLoad / _queenCount;
        public int Capacity => _workQueue.Capacity;

        static IoManualResetValueTaskSource<bool> MallocWorkTaskCore => new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: false, runContinuationsNatively: true);
        static IoManualResetValueTaskSource<bool> MallocQueenTaskCore => new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: false, runContinuationsNatively: false);

        /// <summary>
        /// Queens wake worker threads if there is work to be done. For every worker there is a queen managing it
        ///
        /// This means workers are powered by the threads held by queens in a one to one fashion.
        ///
        /// Workers in turn are powered by their own threads, doing the work. 
        /// </summary>
        /// <param name="this"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        static bool QueenHandler(IoZeroScheduler @this, ZeroSignal s, int id)
        {
            var ts = Environment.TickCount;
#if _TRACE_
            Console.WriteLine($"Queen ASYNC handler... POLLING WORKER..."); 
#endif
           
            //poll a worker
            ThreadPool.UnsafeQueueUserWorkItem(static state =>
            {
                var (@this,s,workerId) = (ValueTuple<IoZeroScheduler, ZeroSignal, int>)state;
               
                if (@this._workQueue.Count == 0 || s.Task!.Status > TaskStatus.WaitingToRun)
                {
                    Interlocked.Exchange(ref s.Processed, 1);
                    s.Task = null;
                    @this._signalHeap.Return(s);
                    return;
                }

                for(var i = @this._workerCount; i -- > 0;)
                {
                    try
                    {
                        if (@this._workerPunchCards[i] == 0)
                        {
                            @this._pollWorker[i].SetResult(true);
                            Interlocked.Exchange(ref s.Processed, 1);
#if _TRACE_
                            Console.WriteLine($"Polled worker {i} from queen {workerId}, for task {s.Task!.Id}");
#endif
                            //return mem
                            s.Task = null;
                            @this._signalHeap.Return(s);
                            return;
                        }
                    }
                    catch
                    {
                        // ignored
                    }
                }
                Interlocked.Exchange(ref s.Processed, 1);
                try
                {
                    //if there is nothing in the Q we don't have to spawn
                    if (@this._workQueue.Count == 0 || s.Task!.Status >= TaskStatus.WaitingToRun)
                        return;

                    if (s.Task != null && @this._lastSpawnedWorker.ElapsedMs() > WorkerSpawnRate && s.Task.Status <= TaskStatus.WaitingToRun)
                    {
                        var newWorkerId = Interlocked.Increment(ref @this._workerCount) - 1;
                        if (newWorkerId < MaxWorker)
                        {
#if __TRACE__
                            Console.WriteLine($"spawning more workers q[{newWorkerId}] = {@this._workQueue.Count}, l = {@this.Load}, {@this.Free.Count()}/{@this.Blocked.Count()}/{@this.Active.Count()}");
#endif

                            Volatile.Write(ref @this._pollWorker[newWorkerId], MallocWorkTaskCore);
                            @this.SpawnWorker($"zero scheduler worker thread {newWorkerId}", newWorkerId, Volatile.Read(ref @this._workQueue), s.Task, callback: WorkerHandler);

                            var newQueenId = Interlocked.Increment(ref @this._queenCount) - 1;
                            Volatile.Write(ref @this._pollQueen[newQueenId], MallocQueenTaskCore);
                            @this.SpawnWorker($"zero scheduler queen thread {newQueenId}", newQueenId, Volatile.Read(ref @this._queenQueue), callback: QueenHandler);

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
                    s.Task = null;
                    @this._signalHeap.Return(s);
                }

            }, (@this,s, id));

            var d = ts.ElapsedMs();
            if(d > 0)
                Console.WriteLine($"QUEEN SPEED => {d} ms");
            return true;
        }

        private static bool WorkerHandler(IoZeroScheduler ioZeroScheduler, Task task, int id)
        {
          //  Debug.Assert(TaskScheduler.Current == ioZeroScheduler);
            var s = task.Status >= TaskStatus.WaitingToRun && ioZeroScheduler.TryExecuteTask(task);
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
        /// <param name="j">worker id</param>
        /// <param name="prime">task to prime this thread with</param>
        /// <param name="queue">The Q o use</param>
        /// <param name="priority">Thread priority</param>
        /// <param name="callback">work handler</param>
        private void SpawnWorker<T>(string desc, int j, IoZeroQ<T> queue, Task prime = null, ThreadPriority priority = ThreadPriority.Normal, Func<IoZeroScheduler, T, int, bool> callback = null) where T : class
        {
            //Thread.CurrentThread.Priority = priority;
#if !DEBUG
#if !__TRACE__
var d = 0;
            if (Active.Any())
            {
                d = ((int)Active.Average()).ElapsedMs();
            }

            Console.WriteLine("-----------------------------------------------------------------------------------------------------------------------------------------");
            Console.WriteLine($"spawning {desc}[{j}], t = {ThreadCount}, l = {Load}/{Environment.ProcessorCount}({(double)Load / Environment.ProcessorCount * 100.0:0.0}%) ({LoadFactor * 100:0.0}%), q = {WLength}, z = {d}ms, a = {Active.Count()}, running = {Blocked.Count()}, Free = {Free.Count()}, ({(double)Blocked.Count() / Active.Count() * 100:0.0}%)");
#endif

#endif
            // ReSharper disable once AsyncVoidLambda
            static async void ThreadWorker(object state)
            {
                try
                {
                    var (@this, queue, desc, workerId, prime, callback, priority) =
                        (ValueTuple<IoZeroScheduler, IoZeroQ<T>, string, int, Task, Func<IoZeroScheduler, T, int, bool>, ThreadPriority>)state;

                    //if (priority >= ThreadPriority.AboveNormal)
                    //    ExecutionContext.SuppressFlow();

                    //Prime task
                    if (prime != null)
                        @this.TryExecuteTask(prime);
#if DEBUG
                    var jobsProcessed = 0;
#endif

                    var taskCore = priority == ThreadPriority.Highest
                        ? Volatile.Read(ref @this._pollQueen[workerId])
                        : Volatile.Read(ref @this._pollWorker[workerId]);
                    //process tasks
                    while (!@this._asyncTasks.IsCancellationRequested)
                    {
                        var blocked = false;
                        try
                        {
                            //wait on work q pressure
                            if (queue.Count == 0 && taskCore.Ready(true))
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
                                blocked = true;
                                var waitForPollCore = new ValueTask<bool>(taskCore, (short)taskCore.Version);

                                if (priority == ThreadPriority.Highest)
                                {
                                    if (!await waitForPollCore.FastPath())
                                    {
#if !_TRACE_
                                        Console.WriteLine(
                                            $"Queen {workerId} unblocking... FAILED! version = {(short)taskCore.Version}");
#endif
                                        continue;
                                    }
                                }

                                if (priority <= ThreadPriority.AboveNormal)
                                {
                                    if (!await waitForPollCore.FastPath())
                                    {
                                        Console.WriteLine(
                                            $"Worker {workerId} unblocking... FAILED! version = {(short)taskCore.Version}");
                                        continue;
                                    }
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


                            //congestion control
                            if (priority == ThreadPriority.Highest)
                            {
                                if (@this.QBlocked.Count() >= MaxLoad)
                                {
                                    await Task.Delay(@this.QLength / @this.QThreadCount * 100);
                                    continue;
                                }

                                @this._lastQueenDispatched = Environment.TickCount;
                            }
                            else
                            {

                                if (@this.Blocked.Count() >= MaxLoad)
                                {
                                    await Task.Delay(@this.WLength / @this.ThreadCount * 100);
                                    continue;
                                }

                                @this._lastWorkerDispatched = Environment.TickCount;
                            }
#if _TRACE_
                            if(queue.Count > 0)
                                Console.WriteLine($"{desc}[{workerId}]: Q size = {queue.Count}, priority = {priority}, {jobsProcessed}");
#endif

                            try
                            {
//Service the Q
                                if (priority == ThreadPriority.Highest)
                                {
                                    Interlocked.Increment(ref @this._queenPunchCards[workerId]);
#if __TRACE__
                                Console.WriteLine($"[[QUEEN]] CONSUMING FROM Q {workerId},  Q = {queue.Count}/{@this.Active.Count()} - total jobs process = {@this.CompletedWorkItemCount}");
#endif
                                }
                                else
                                {
                                    Interlocked.Increment(ref @this._workerPunchCards[workerId]);
                                }

                                while (queue.Count > 0 && queue.TryDequeue(out var work))
                                {
                                    try
                                    {
                                        if (callback(@this, work, workerId))
                                        {
                                            if (priority == ThreadPriority.Highest)
                                                Interlocked.Increment(ref @this._completedQItemCount);
                                            else
                                                Interlocked.Increment(ref @this._completedWorkItemCount);
#if DEBUG
                                            jobsProcessed++;
#endif
                                        }
                                    }
                                    catch (Exception e)
                                    {
#if DEBUG
                                        LogManager.GetCurrentClassLogger().Error(e,
                                            $"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this}, wq = {@this?._workQueue}, work = {work}, q = {taskCore}");
#endif

                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                            finally
                            {
                                if (priority == ThreadPriority.Normal)
                                {
                                    Interlocked.Decrement(ref @this._workerPunchCards[workerId]);
                                }
                                else if (priority == ThreadPriority.Highest)
                                {
                                    Interlocked.Decrement(ref @this._queenPunchCards[workerId]);
                                }
                            }
                        }
                        catch (Exception e)
                        {
#if DEBUG
                            LogManager.GetCurrentClassLogger().Error(e,
                                $"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this != null}, wq = {@this?._workQueue}, q = {taskCore != null}");
#endif
                        }
                        finally
                        {
                            if(blocked)
                                taskCore.Reset();
                        }

                        //drop zombie workers
                        if (priority == ThreadPriority.Normal && workerId == @this._workerCount && @this._dropWorker > 0)
                        {
                            if (Interlocked.Decrement(ref @this._dropWorker) >= 0)
                            {
                                if (@this._workerPunchCards[workerId] > 0)
                                {
#if !__TRACE__
                                    Console.WriteLine($"!!!! KILLED WORKER THREAD ~> {desc}ZW[{workerId}], processed = {@this.CompletedWorkItemCount}");
#endif
                                }

                                break;
                            }
                        }

                        //Oneshot workers end here.
                        if (priority == ThreadPriority.AboveNormal)
                        {
                            break;
                        }
                    }

                    if (priority >= ThreadPriority.Normal)
                    {
                        Console.WriteLine($"KILLING WORKER THREAD id ={workerId} - has = {desc} -> wants = ({priority})");

                        if (priority == ThreadPriority.Highest)
                            Volatile.Write(ref @this._queenPunchCards[workerId], -1);
                        else
                            Volatile.Write(ref @this._workerPunchCards[workerId], -1);

                        var wTmp = @this._pollWorker[workerId];
                        var qTmp = @this._pollQueen[workerId];
                        @this._pollQueen[workerId] = null;
                        @this._pollWorker[workerId] = null;

                        try
                        {
                            qTmp?.SetException(
                                new ThreadInterruptedException($"Inactive thread queenId = {workerId} was purged"));
                        }
                        catch
                        {
                            // ignored
                        }

                        try
                        {
                            wTmp.SetException(
                                new ThreadInterruptedException($"Inactive thread workerId = {workerId} was purged"));
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

            if (priority == ThreadPriority.Highest)
                Volatile.Write(ref _queenPunchCards[j], 0);
            else
                Volatile.Write(ref _workerPunchCards[j], 0);

            //if (priority >= ThreadPriority.AboveNormal)
            {
                var t = new Thread(ThreadWorker)
                {
                    IsBackground = true,
                    Name = $"{desc}"
                };
                t.Start((this, queue, desc, j, prime, callback, priority));
            }
            //else
            {
               // _ = Task.Factory.StartNew(ThreadWorker, (this, queue, desc, j, prime, callback, priority), CancellationToken.None, TaskCreationOptions.None, Default);
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
        protected override void QueueTask(Task task)
        {
#if _TRACE_
            Console.WriteLine($"<--- Queueing task id = {task.Id}, {task.Status}");
#endif

            if (task.Status > TaskStatus.WaitingToRun)
                return;

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
            _workQueue.TryEnqueue(task);

            PollQueen(task);
        }

        /// <summary>
        /// polls a queen
        /// </summary>
        /// <param name="task">A task associated with this poll</param>
        /// <returns>True if a poll was sent, false otherwise</returns>
        private bool PollQueen(Task task)
        {
            //if there is a queen to be polled
            if (_queenCount - QBlocked.Count() <= 0)
                return false;

            ZeroSignal zeroSignal = null;
            var polled = false;
            try
            {
                zeroSignal = _signalHeap.Take();
                Debug.Assert(zeroSignal != null);

                //prepare work queen poll signal
                zeroSignal.Task = task;

                var tmpSignal = zeroSignal;
                if (task.Status <= TaskStatus.WaitingToRun)
                    _queenQueue.TryEnqueue(tmpSignal);
                else
                    return true;

                zeroSignal = null;

                //poll a queen that there is work to be done, forever
                var qId = _queenCount;
                while (!polled && task.Status <= TaskStatus.WaitingToRun && qId-- > 0)
                {
                    if (tmpSignal.Processed > 0)
                    {
                        polled = true;
                        break;
                    }

                    var q = _pollQueen[qId];
                    try
                    {
                        if (tmpSignal.Processed == 0)
                        {
                            //if (q.Ready())
                            {
                                q.SetResult(true);
#if _TRACE_
                                Console.WriteLine($"load = {QLoad}, Polled queen {qId} for task id {task.Id}");
#endif
                                polled = true;
                                break;
                            }
                        }
                    }
                    catch
                    {
                        polled = true;
                        // ignored
                    }
                }

                if (!polled && task.Status <= TaskStatus.WaitingToRun)
                    Console.WriteLine($"Unable to poll any of the {_queenCount} queens. backlog = {_queenQueue.Count}, Load = {QLoad} !!!!!!!!");
                
            }
            finally
            {
                if (zeroSignal != null)
                {
                    zeroSignal.Task = null;
                    _signalHeap.Return(zeroSignal);
                }
            }

            return polled;
        }

        /// <summary>
        /// Tries to execute the task inline
        /// </summary>
        /// <param name="task">The task the execute</param>
        /// <param name="taskWasPreviouslyQueued">If the task was previously queued</param>
        /// <returns>true if executed, false otherwise</returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            var s = TryExecuteTask(task);
            if (s)
            {
#if __TRACE__
                Console.WriteLine($"task id = {task.Id}, {task.Status} INLINED!");
#endif
            }
            else
            {
#if !__TRACE__
                Console.WriteLine($"task id = {task.Id}, {task.Status} INLINE FAILED!");
#endif          
            }

            return s;
        }
    }
}
