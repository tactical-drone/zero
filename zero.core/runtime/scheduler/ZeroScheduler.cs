using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;

namespace zero.core.runtime.scheduler
{
    /// <summary>
    /// Experimental task scheduler based on zero tech
    /// </summary>
    public class IoZeroScheduler : TaskScheduler
    {
        static IoZeroScheduler()
        {
            Zero = new IoZeroScheduler();
            ZeroDefault = Default;

            //ZeroDefault = Zero;
        }
        public IoZeroScheduler(CancellationTokenSource asyncTasks = null)
        {
            _ = base.Id; // force ID creation of the default scheduler

            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Environment.ProcessorCount >> 1;

            Volatile.Write(ref _workQueue, new IoBag<Task>(string.Empty, _workerCount * 2, true));
            Volatile.Write(ref _qUp, new IoManualResetValueTaskSource<bool>[MaxWorker]);
            Volatile.Write(ref _workerPunchCards, new int[MaxWorker]);

            for (var i = 0; i < _workerCount; i++)
            {
                _qUp[i] = new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously:true, runContinuationsNatively:false);
                SpawnWorker(i);
            }
        }

        private static readonly int WorkerSpawnRate = 16*10;
        private static readonly int ExpireTimeout = 60;
        private static readonly int MaxLoad = Environment.ProcessorCount * 10;
        private static readonly int MaxWorker = short.MaxValue;
        public static readonly TaskScheduler ZeroDefault;
        public static readonly IoZeroScheduler Zero;
        private readonly CancellationTokenSource _asyncTasks;
        private readonly IoManualResetValueTaskSource<bool>[] _qUp;
        private readonly int[] _workerPunchCards;
        private volatile int _dropWorker;
        private readonly IoBag<Task> _workQueue;
        private volatile int _workerCount;
        private int _load;
        private long _completedWorkItemCount;
        private volatile int _lastSpawnedWorker;
        public int Active;
        public int Blocked;
        public int Running;

        public int Load => Volatile.Read(ref _load);
        public int QLength => _workQueue.Count;
        public int ThreadCount => _workerCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public double LoadFactor => (double) Volatile.Read(ref _load) / _workerCount;
        public int CurrentCapacity => _workQueue.Capacity;

        private void SpawnWorker(int j, Task prime = null)
        {
            var cards = _workerPunchCards.Take(_workerCount);
            var active = cards.Where(t => t > 0).ToList();
            Blocked = active.Count(t => t.ElapsedMs() > 5000);
            Active = active.Count;
            Running = Active - Blocked;

            int d = 0;
            if (active.Any())
            {
                d = ((int)active.Average()).ElapsedMs();
            }

            Console.WriteLine($"ZW[{j}], t = {ThreadCount}, l = {Load}/{Environment.ProcessorCount}({(double)Load / Environment.ProcessorCount * 100.0:0.0}%) ({LoadFactor * 100:0.0}%), q = {QLength}, z = {d}ms, a = {Active}, r = {Running}, w = {Blocked} ({(double)Blocked / Active * 100:0.0}%)");

            _ = Task.Factory.StartNew(static async state =>
            {
                var (@this, workerId, prime) = (ValueTuple<IoZeroScheduler, int, Task>)state;

                //Prime task
                if (prime != null)
                    @this.TryExecuteTask(prime);

                while (!@this._asyncTasks.IsCancellationRequested)
                {
                    //drop zombie workers
                    if (workerId == @this._workerCount && @this._dropWorker > 0)
                    {
                        if (Interlocked.Decrement(ref @this._dropWorker) >= 0)
                        {
                            if (@this._workerPunchCards[workerId] > 0)
                            {
                                var s = TimeSpan.FromSeconds(@this._workerPunchCards[workerId].ElapsedMs()).TotalSeconds;
                                Console.WriteLine($"!!!! KILLED THREAD ~> ZW[{workerId}], age = {s}s");
                            }
                            break;
                        }
                        Interlocked.Increment(ref @this._dropWorker);
                    }
                    
                    var q = Volatile.Read(ref @this._qUp[workerId]);

                    try
                    {
                        if (q == null)
                        {
                            Console.WriteLine($"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this}, wq = {@this?._workQueue}, q = {q}");
                            Console.WriteLine("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                            Console.WriteLine("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                            break;
                        }
                        
                        if (@this._workQueue.Count == 0 && q.Set(true) && //(q?.Set(true)??false) && //TODO: why do these do much worse when they should be doing better?
                        !await new ValueTask<bool>(q, (short)q.Version).ConfigureAwait(false))
                            break;

                        var slow = @this._workerPunchCards.Take(@this._workerCount).Where(t => t > 0).Count(t => t.ElapsedMs() > 5000);

                        if (Interlocked.Increment(ref @this._load) - slow >= MaxLoad)
                        {
                            Interlocked.Decrement(ref @this._load);
                            await Task.Delay(@this.QLength / @this.ThreadCount * 100).ConfigureAwait(false);
                            continue;
                        }

                        while (@this._workQueue.TryTake(out var work) && work != null) //TODO: what is going on here?
                        {
                            try
                            {
                                if (work.Status <= TaskStatus.WaitingToRun)
                                {
                                    @this._workerPunchCards[workerId] = Environment.TickCount;
                                    if (@this.TryExecuteTask(work))
                                        Interlocked.Increment(ref @this._completedWorkItemCount);
                                    else
                                    {
                                        var cards = @this._workerPunchCards.Take(@this._workerCount);
                                        var active = cards.Where(t => t > 0).ToList();
                                        @this.Blocked = active.Count(t => t.ElapsedMs() > 5000);
                                        @this.Active = active.Count;
                                        @this.Running = @this.Active - @this.Blocked;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
#if DEBUG
                                LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this}, wq = {@this?._workQueue}, work = {work}, q = {q}");
#endif
                            }
                        }

                        //signal to drop another thread, we don't drop this one because of performance reasons so we drop the latest thread made
                        if (@this._workerPunchCards[workerId] > 0)
                        {
                            var wallClock = @this._workerPunchCards[workerId].ElapsedMsToSec();
                            if (wallClock > ExpireTimeout)
                            {
                                Interlocked.Increment(ref @this._dropWorker);
                            }
                        }
                    }
                    catch (Exception e)
                    {
#if DEBUG
                        LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this != null}, wq = {@this?._workQueue}, q = {q != null}");               
#endif
                    }
                    finally
                    {
                        @this._qUp[workerId].Reset();
                        Interlocked.Decrement(ref @this._load);
                    }
                }

                var qTmp = @this._qUp[workerId];
                @this._qUp[workerId] = null;

                try
                {
                    qTmp.Reset();
                    qTmp.SetException(new ThreadInterruptedException($"Inactive thread {workerId} was purged"));
                    //Console.WriteLine($"!!!! KILLED THREAD ~> ZW[{workerId}], t = {@this.ThreadCount}, l = {@this.Load}/{Environment.ProcessorCount}({(double)@this.Load / Environment.ProcessorCount * 100.0:0.0}%) ({@this.LoadFactor * 100:0.0}%), q = {@this.QLength})");
                }
                catch
                {
                    // ignored
                }
            }, (this, j, prime), CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, Default);

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _workQueue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void QueueTask(Task task)
        {
            if (task.Status > TaskStatus.WaitingToRun)
                return;

            _workQueue.Add(task);
            var scheduled = false;

            for (var i = _workerCount; i-- > 0;)
            {
                if(task.Status > TaskStatus.WaitingToRun)
                    break;
                
                var q = _qUp[i];

                try
                {
                    if (q == null || q.GetStatus((short)q.Version) != ValueTaskSourceStatus.Pending) continue;
                    q.SetResult(true);
                    scheduled = true;
                    break;
                }
                catch
                {
                    // ignored
                }
            }

            if (!scheduled && _lastSpawnedWorker.ElapsedMs() > WorkerSpawnRate && task.Status <= TaskStatus.WaitingToRun)
            {
                var newWorkerId = Interlocked.Increment(ref _workerCount);
                if (newWorkerId < MaxWorker)
                {
                    Volatile.Write(ref _qUp[newWorkerId], new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: true, runContinuationsNatively: false));
                    SpawnWorker(newWorkerId, task);
                    //scheduled = true;
                    _lastSpawnedWorker = Environment.TickCount;
                }
                else
                {
                    Interlocked.Decrement(ref _workerCount);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            //try
            //{
            //    task.RunSynchronously(Default);
            //    return true;
            //}
            //catch
            //{
            //    // ignored
            //}

            //return false;
            return TryExecuteTask(task);
        }
    }
}
