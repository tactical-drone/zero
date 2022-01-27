using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
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
            ZeroDefault = new IoZeroScheduler();
            //ZeroDefault = TaskScheduler.Default;
        }
        public IoZeroScheduler(CancellationTokenSource asyncTasks = null)
        {
            _asyncTasks = asyncTasks?? new CancellationTokenSource();
            _workerCount = Environment.ProcessorCount * 2;
            _workQueue = new IoBag<Task>(string.Empty, _workerCount * 2, true);

            _qUp = new IoManualResetValueTaskSource<bool>[_maxWorker];

            for (var i = 0; i < _workerCount; i++)
            {
                _qUp[i] = new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously:false);
                SpawnWorker(i);
            }
        }

        private void SpawnWorker(int j, Task prime = null)
        {
            Task.Factory.StartNew(static async state =>
            {
                var (@this, workerId, prime) = (ValueTuple<IoZeroScheduler, int, Task>)state;

                //Prime task
                if (prime != null)
                    @this.TryExecuteTask(prime);

                while (!@this._asyncTasks.IsCancellationRequested)
                {
                    try
                    {
                        if (@this._workQueue.Count == 0 &&
                            !await new ValueTask<bool>(@this._qUp[workerId], @this._qUp[workerId].Version).ConfigureAwait(false))
                            break;

                        Interlocked.Increment(ref @this._load);
                        while (@this._workQueue.TryTake(out var work))
                        {
                            if (work.Status <= TaskStatus.WaitingToRun)
                                @this.TryExecuteTask(work);
                        }
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoZeroScheduler)}");
                    }
                    finally
                    {
                        Interlocked.Decrement(ref @this._load);
                        @this._qUp[workerId].Reset();
                    }
                }
            }, (this, j, prime), CancellationToken.None, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, Default);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return _workQueue;
        }

        private static readonly int _maxWorker = short.MaxValue;
        public static readonly TaskScheduler ZeroDefault;
        private readonly CancellationTokenSource _asyncTasks;
        private readonly IoManualResetValueTaskSource<bool>[] _qUp;
        private readonly IoBag<Task> _workQueue;
        private volatile int _workerCount;
        private int _load;
        public double LoadFactor => (double) Volatile.Read(ref _load) / _workerCount;
        public int CurrentCapacity => _workQueue.Capacity;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void QueueTask(Task task)
        {
            _workQueue.Add(task);
            ThreadPool.QueueUserWorkItem(static state =>
            {
                var scheduled = false;
                var (@this, task) = (ValueTuple<IoZeroScheduler, Task>)state;

                for (var i = 0; i < @this._workerCount; i++)
                {
                    if(task.Status > TaskStatus.WaitingToRun)
                        break;
                    
                    var q = @this._qUp[i];

                    try
                    {
                        if (q == null || q.GetStatus(q.Version) != ValueTaskSourceStatus.Pending) continue;
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
                    if (@this._workerCount <= _maxWorker)
                    {
                        Console.WriteLine("Adding new worker...");
                        @this._qUp[newWorkerId] = new IoManualResetValueTaskSource<bool>(runContinuationsAsynchronously: false);
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
            return TryExecuteTask(task);
        }
    }
}
