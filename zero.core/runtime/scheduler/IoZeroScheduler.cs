using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
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

            ZeroDefault = Default; //TODO: disabled for now, strange fundamental failures occurring.
        }
        public IoZeroScheduler(CancellationTokenSource asyncTasks = null)
        {
            _ = base.Id; // force ID creation of the default scheduler
            
            if(!Enabled)
                return;
            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Math.Max(Environment.ProcessorCount >> 1, 2);
            _queenCount = Math.Max(Environment.ProcessorCount >> 1, 2);
            var capacity = MaxWorker;

            _workQueue = new IoZeroQ<Task>(string.Empty, capacity, new Task(() => {}), false);
            _queenQueue = new IoZeroQ<ZeroSignal>(string.Empty, capacity, new ZeroSignal(), false);
            _signalHeap = new IoHeap<ZeroSignal>(string.Empty, capacity, (_, _) => new ZeroSignal(), false)
            {
                PopAction = (signal, _) =>
                {
                    signal.Processed = 0;
                }
            };

            _diagnosticsHeap = new IoHeap<List<int>>(string.Empty, capacity, (context, _) => new List<int>(context is int i ? i : 0), false)
            {
                PopAction = (list, _) =>
                {
                    list.Clear();
                }
            };

            _pollWorker = new IoManualResetValueTaskSource<bool>[MaxWorker];
            _pollQueen = new IoManualResetValueTaskSource<bool>[MaxWorker];
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

            //TODO: Why does a thread here cause catastrophic failure of code execution?
            //new Thread(
            Task.Factory.StartNew(static state =>
            {
                var @this = (IoZeroScheduler)state;
                while (!@this._asyncTasks.IsCancellationRequested)
                {
                    try
                    {
                        if (@this._workQueue.Count > 0 && @this._lastWorkerDispatched.ElapsedMs() > WorkerSpawnRate)
                        {
                            _ = Task.Factory.StartNew(static state =>
                            {
                                var @this = (IoZeroScheduler)state;
                                if (@this._workQueue.TryPeek(out var head))
                                {
                                    var active = @this.Active;

                                    try
                                    {
                                        int w;
                                        for (w = 0; w < active.Count; w++)
                                        {
                                            if (active[w] != 0) continue;

                                            try
                                            {
                                                @this._pollWorker[w].SetResult(true);
                                            }
                                            catch
                                            {
                                                if (@this.PollQueen(head))
                                                {
                                                    Console.WriteLine(
                                                        $"added zero scheduler (one-shot) worker thread task id = {head.Id} , status = {head.Status}");
                                                }
                                            }
                                            break;
                                        }
                                    }
                                    finally
                                    {
                                        @this._diagnosticsHeap.Return(active);   
                                    }
                                }
                            }, @this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, Default);
                        }

                        Thread.Sleep(WorkerSpawnRate);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }, this, CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, Default);
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
        private static readonly int WorkerSpawnRate = 16*20;
        private static readonly int WorkerExpireThreshold = 10;
        private static readonly int MaxLoad = Environment.ProcessorCount * 10;

        private static readonly int MaxWorker = (int)Math.Pow(2, Math.Max((Environment.ProcessorCount>>1) + 1, 17));
        public static readonly TaskScheduler ZeroDefault;
        public static readonly IoZeroScheduler Zero;
        private CancellationTokenSource _asyncTasks;
        private IoManualResetValueTaskSource<bool>[] _pollWorker;
        private IoManualResetValueTaskSource<bool>[] _pollQueen;
        private int[] _workerPunchCards;
        private int[] _queenPunchCards;
        private volatile int _dropWorker;
        private IoZeroQ<Task> _workQueue;
        private IoZeroQ<ZeroSignal> _queenQueue;
        private IoHeap<ZeroSignal> _signalHeap;
        private IoHeap<List<int>> _diagnosticsHeap;

        private volatile int _workerCount;
        private volatile int _queenCount;
        private volatile int _lastWorkerDispatched;
        private volatile int _lastQueenDispatched;
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

        static IoManualResetValueTaskSource<bool> MallocWorkTaskCore => new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: false, runContinuationsNatively: true);
        static IoManualResetValueTaskSource<bool> MallocQueenTaskCore => new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: false, runContinuationsNatively: true);

        /// <summary>
        /// Queens wake worker threads if there is work to be done. For every worker there is a queen managing it
        /// 
        /// This means workers are powered by the threads held by queens in a one to one fashion.
        /// 
        /// Workers in turn are powered by their own threads, doing the work. 
        /// </summary>
        /// <param name="this">The scheduler</param>
        /// <param name="s">The signal to be sent</param>
        /// <param name="id">The queen id</param>
        /// <returns>True if successful, false otherwise</returns>
        static bool QueenHandler(IoZeroScheduler @this, ZeroSignal s, int id)
        {
            var ts = Environment.TickCount;
#if _TRACE_
            Console.WriteLine($"Queen ASYNC handler... POLLING WORKER..."); 
#endif

            try
            {
                if (s.Processed > 0 || s.Task.Status > TaskStatus.WaitingToRun)
                    return false;
            }
            catch when(s.Processed > 0){}
            catch (Exception e) when(s.Processed == 0)
            {
                Console.WriteLine($"{e.Message}, s = {s}, p = {Volatile.Read(ref s.Processed)}, task = {s?.Task}, status = {s?.Task?.Id}");
                throw;
            }

            //poll a worker, or create a new one if none are available
            try
            {
                if (!ThreadPool.UnsafeQueueUserWorkItem(static state =>
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
                                for (var i = @this._workerCount; i-- > 0;)
                                {
                                    try
                                    {
                                        var w = @this._pollWorker[i];
                                        if (@this._workerPunchCards[i] == 0 && w.Ready())
                                        {
                                            w.SetResult(true);

#if _TRACE_
                                            Console.WriteLine($"Polled worker {i} from queen {workerId}, for task {s.Task!.Id}");
#endif
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
                            if (@this._lastSpawnedWorker.ElapsedMs() >
                                WorkerSpawnRate || blocked.Count >= @this._workerCount)
                            {
                                var newWorkerId = Interlocked.Increment(ref @this._workerCount) - 1;
                                if (newWorkerId < MaxWorker)
                                {
#if __TRACE__
                                Console.WriteLine($"spawning more workers q[{newWorkerId}] = {@this._workQueue.Count}, l = {@this.Load}, {@this.Free.Count()}/{@this.Blocked.Count()}/{@this.Active.Count()}");
#endif
                                    Volatile.Write(ref @this._pollWorker[newWorkerId], MallocWorkTaskCore);
                                    var spawnWorker = @this.SpawnWorker<Task>;
                                    spawnWorker($"zero scheduler worker thread {newWorkerId}", newWorkerId,
                                        @this._workQueue, s.Task, ThreadPriority.Normal, WorkerHandler);

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

                    }, (@this, s, id)))
                {
                    Console.WriteLine($"Queen[{id}]: Unable to Q signal for task {s.Task.Id}, {ts.ElapsedMs()}ms, OOM");
                    return false;
                }
            }
            catch when (s.Processed > 1) { return false;}
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

            var d = ts.ElapsedMs();
            if(d > 1)
                Console.WriteLine($"QUEEN{id} SLOW => {d}ms, q length = {@this.QLength}");
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
#if DEBUG
#if __TRACE__
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

                    //fast prime task to unwind current worker dead lockers
                    if (prime is { Status: <= TaskStatus.WaitingToRun })
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
                        var taskCoreUsed = false;
                        try
                        {
                            var errStr = "";
                            errStr += taskCore.GetStatus((short)taskCore.Version);
                            errStr += ", ";
                            //wait on work q pressure
                            if (queue.Count == 0 && taskCore.Ready(true))
                            {
                                errStr += taskCore.GetStatus((short)taskCore.Version);
                                errStr += ", ";
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

                                errStr += taskCore.GetStatus((short)taskCore.Version);
                                errStr += ", ";
                                var waitForPollCore = new ValueTask<bool>(taskCore, (short)taskCore.Version);
                                errStr += taskCore.GetStatus((short)taskCore.Version);
                                errStr += ", ";
                                taskCoreUsed = true;
                                if (priority == ThreadPriority.Highest)
                                {
                                    if (!await waitForPollCore.FastPath().ConfigureAwait(false))
                                    {
#if !_TRACE_
                                        Console.WriteLine($"Queen {workerId} unblocking... FAILED! status = {taskCore.GetStatus((short)taskCore.Version)}, {errStr}");
#endif
                                        continue;
                                    }
                                }

                                if (priority <= ThreadPriority.AboveNormal)
                                {
                                    if (!await waitForPollCore.FastPath())
                                    {
#if !_TRACE_
                                        Console.WriteLine($"Worker {workerId} unblocking... FAILED! version = {taskCore.GetStatus((short)taskCore.Version)}, {errStr}");
#endif
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
                            else
                            {
                                
                            }


                            ////congestion control
                            //if (priority == ThreadPriority.Highest)
                            //{
                            //    var qBlocked = @this.QBlocked;
                            //    try
                            //    {
                            //        if (qBlocked.Count >= MaxLoad)
                            //        {
                            //            await Task.Delay(@this.QLength / @this.QThreadCount * 100);
                            //            continue;
                            //        }
                            //    }
                            //    finally
                            //    {
                            //        @this._diagnosticsHeap.Return(qBlocked);
                            //    }

                            //    @this._lastQueenDispatched = Environment.TickCount;
                            //}
                            //else
                            //{
                            //    var blocked = @this.Blocked;
                            //    try
                            //    {
                            //        if (blocked.Count >= MaxLoad)
                            //        {
                            //            await Task.Delay(@this.WLength / @this.ThreadCount * 100);
                            //            continue;
                            //        }
                            //    }
                            //    finally
                            //    {
                            //        @this._diagnosticsHeap.Return(blocked);
                            //    }

                            //    @this._lastWorkerDispatched = Environment.TickCount;
                            //}
                            @this._lastQueenDispatched = Environment.TickCount;
                            @this._lastWorkerDispatched = Environment.TickCount;
#if _TRACE_
                            if(queue.Count > 0)
                                Console.WriteLine($"{desc}[{workerId}]: Q size = {queue.Count}, priority = {priority}, {jobsProcessed}");
#endif

                            try
                            {
//Service the Q
                                if (priority == ThreadPriority.Highest)
                                {
                                    Interlocked.Exchange(ref @this._queenPunchCards[workerId], 1);
#if __TRACE__
                                Console.WriteLine($"[[QUEEN]] CONSUMING FROM Q {workerId},  Q = {queue.Count}/{@this.Active.Count()} - total jobs process = {@this.CompletedWorkItemCount}");
#endif
                                }
                                else
                                {
                                    Interlocked.Exchange(ref @this._workerPunchCards[workerId], 1);
                                }

                                while (queue.TryDequeue(out var work))
                                {
                                    Debug.Assert(work != null);
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
                                Console.WriteLine($"---> status = {taskCore.GetStatus((short)taskCore.Version)}");
                            }
                            finally
                            {
                                if (taskCoreUsed)
                                {
                                    taskCoreUsed = false;
                                    taskCore.Reset();
                                }
                                else
                                {
                                    taskCoreUsed = true;
                                }
                                if (priority == ThreadPriority.Normal)
                                {
                                    Interlocked.Exchange(ref @this._workerPunchCards[workerId], 0);
                                }
                                else if (priority == ThreadPriority.Highest)
                                {
                                    Interlocked.Exchange(ref @this._queenPunchCards[workerId], 0);
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
                            if(taskCoreUsed)
                                taskCore.Reset();
                        }

                        //drop zombie workers
                        if (priority == ThreadPriority.Normal && workerId == @this._workerCount && @this._dropWorker > 0)
                        {
                            if (Interlocked.Decrement(ref @this._dropWorker) >= 0)
                            {
#if !__TRACE__
                                Console.WriteLine(
                                    $"!!!! KILLED WORKER THREAD ~> {desc}ZW[{workerId}], processed = {@this.CompletedWorkItemCount}");
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

                    if (priority == ThreadPriority.Normal)
                    {
                        Console.WriteLine($"KILLING WORKER THREAD id ={workerId} - has = {desc})");
                        Volatile.Write(ref @this._workerPunchCards[workerId], -1);

                        var wTmp = @this._pollWorker[workerId];
                        @this._pollWorker[workerId] = null;

                        try
                        {
                            wTmp.SetException(new ThreadInterruptedException($"Inactive thread workerId = {workerId} was purged"));
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

            var t = new Thread(ThreadWorker)
            {
                IsBackground = true,
                Name = $"{desc}"
            };
            t.Start((this, queue, desc, j, prime, callback, priority));
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
            Debug.Assert(task != null);
            //if there is a queen to be polled
            var qBlocked = QBlocked;
            try
            {
                if (_queenCount - qBlocked.Count() <= 0 || QLength > MaxWorker>>1)//TODO: What is going on here?
                    return false;
            }
            finally
            {
                _diagnosticsHeap.Return(qBlocked);
            }

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
                }
                else
                    return true;
            }
            finally
            {
                if(zeroSignal != null)
                    _signalHeap.Return(zeroSignal);
            }

            if (zeroSignal != null)
                return false;
            
            //poll a queen that there is work to be done, forever
            var qId = _queenCount;
            while (!polled && task.Status <= TaskStatus.WaitingToRun && qId-- > 0)
            {
                if (tmpSignal.Processed > 0)
                {
                    polled = true;
                    break;
                }

                if (_queenPunchCards[qId] == 1)
                    continue;

                var q = _pollQueen[qId];
                try
                {
                    if (tmpSignal.Processed == 0 && _queenPunchCards[qId] == 0)
                    {
                        if (q.Ready())
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
                    //polled = true;
                    // ignored
                }
            }

#if __TRACE__
            if (!polled && task.Status <= TaskStatus.WaitingToRun)
                Console.WriteLine($"Unable to poll any of the {_queenCount} queens. backlog = {_queenQueue.Count}, Load = {QLoad} !!!!!!!!");
#endif
            

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

        /// <summary>
        /// returns a diagnostic result back into the heap
        /// </summary>
        /// <param name="value"></param>
        public void Return(List<int> value)
        {
            _diagnosticsHeap.Return(value);
        }
    }
}
