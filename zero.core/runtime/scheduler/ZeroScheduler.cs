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
            //ZeroDefault = Zero;
            ZeroDefault = Default;
        }
        public IoZeroScheduler(CancellationTokenSource asyncTasks = null)
        {
            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Environment.ProcessorCount >> 1;
            Volatile.Write(ref _workQueue, new IoBag<Task>(string.Empty, _workerCount * 2, true));
            //_workQueue = new IoBag<Task>(string.Empty, _workerCount * 2, true);

            Volatile.Write(ref _qUp, new IoManualResetValueTaskSource<bool>[MaxWorker]);
            Volatile.Write(ref _workerPunchCards, new int[MaxWorker]);

            for (var i = 0; i < _workerCount; i++)
            {
                _qUp[i] = new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously:false, runContinuationsNatively:false);
                SpawnWorker(i);
            }
        }

        private void SpawnWorker(int j, Task prime = null)
        {
            var cards = _workerPunchCards.Take(_workerCount);
            var active = cards.Where(t=> t > 0).ToList();
            var slow = active.Count(t => t.ElapsedMs() > 5000);
            var activeCount = active.Count;
            var running = activeCount - slow;
            
            int d = 0;
            if (active.Any())
            {
                d = ((int)active.Average()).ElapsedMs();
            }

            Console.WriteLine($"ZW[{j}], t = {ThreadCount}, l = {Load}/{Environment.ProcessorCount}({(double)Load/Environment.ProcessorCount * 100.0:0.0}%) ({LoadFactor * 100:0.0}%), q = {QLength}, z = {d}ms, a = {activeCount}, r = {running}, w = {slow} ({(double)slow/activeCount*100:0.0}%)");
            
            Task.Factory.StartNew(static async state =>
            {
                var (@this, workerId, prime) = (ValueTuple<IoZeroScheduler, int, Task>)state;

                if (@this == null)
                    throw new ArgumentNullException($"{nameof(state)}: this was null;");

                //Prime task
                if (prime != null)
                    @this.TryExecuteTask(prime);
                
                while (!@this._asyncTasks.IsCancellationRequested)
                {
                    if (@this == null)
                        throw new ArgumentNullException($"{nameof(state)}: this was null;");
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
                        q.Set(true);
                        if (@this._workQueue.Count == 0 && //q.Set(true) && //(q?.Set(true)??false) && //TODO: why do these do much worse when they should be doing better?
                        !await new ValueTask<bool>(q, (short)q.Version).ConfigureAwait(false))
                            break;

                        var slow = @this._workerPunchCards.Take(@this._workerCount).Where(t => t > 0).Count(t => t.ElapsedMs() > 5000);

                        if (Interlocked.Increment(ref @this._load) - slow >= Environment.ProcessorCount)
                        {
                            Interlocked.Decrement(ref @this._load);
                            await Task.Delay(@this.QLength/@this.ThreadCount * 100).ConfigureAwait(false);
                            continue;
                        }

                        while (@this._workQueue.TryTake(out var work) && work != null) //TODO: what is going on here?
                        {
                            try
                            {
                                if (work.Status <= TaskStatus.WaitingToRun)
                                {
                                    @this._workerPunchCards[workerId] = Environment.TickCount;
                                    if(@this.TryExecuteTask(work))
                                        Interlocked.Increment(ref @this._completedWorkItemCount);
                                }
                            }
                            catch (Exception e)
                            {
                                LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this}, wq = {@this?._workQueue}, work = {work}, q = {q}");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoZeroScheduler)}: wId = {workerId}/{@this._workerCount}, this = {@this != null}, wq = {@this?._workQueue}, q = {q != null}");
                    }
                    finally
                    {
                        @this._qUp[workerId].Reset();
                        Interlocked.Decrement(ref @this._load);
                    }
                }
            }, (this, j, prime), CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, Default);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _workQueue;
        }

        private static readonly int MaxWorker = short.MaxValue;
        public static readonly TaskScheduler ZeroDefault;
        public static readonly IoZeroScheduler Zero;
        private readonly CancellationTokenSource _asyncTasks;
        private readonly IoManualResetValueTaskSource<bool>[] _qUp;
        private readonly int[] _workerPunchCards;
        private readonly IoBag<Task> _workQueue;
        private volatile int _workerCount;
        private int _load;
        private long _completedWorkItemCount;
        public int Load => Volatile.Read(ref _load);
        public int QLength => _workQueue.Count;
        public int ThreadCount => _workerCount;
        public long CompletedWorkItemCount => _completedWorkItemCount;
        public double LoadFactor => (double) Volatile.Read(ref _load) / _workerCount;
        public int CurrentCapacity => _workQueue.Capacity;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void QueueTask(Task task)
        {
            _workQueue.Add(task);
            ThreadPool.UnsafeQueueUserWorkItem(static state =>
            {
                var scheduled = false;
                var (@this, task) = (ValueTuple<IoZeroScheduler, Task>)state;

                if (task.Status > TaskStatus.WaitingToRun)
                    return;

                for (var i = 0; i < @this._workerCount; i++)
                {
                    if(task.Status > TaskStatus.WaitingToRun)
                        break;
                    
                    var q = @this._qUp[i];

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

                if (!scheduled && task.Status <= TaskStatus.WaitingToRun)
                {
                    var newWorkerId = Interlocked.Increment(ref @this._workerCount);
                    if (@this._workerCount < MaxWorker)
                    {
                        Volatile.Write(ref @this._qUp[newWorkerId], new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: true, runContinuationsNatively: false));
                        Thread.MemoryBarrier();
                        @this.SpawnWorker(newWorkerId, task);
                    }
                    else
                    {
                        Interlocked.Decrement(ref @this._workerCount);
                    }
                }
            }, (this, task));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            //try
            //{
            //    task.RunSynchronously(this);
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
