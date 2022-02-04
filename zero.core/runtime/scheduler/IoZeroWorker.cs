using System.Threading.Tasks;

namespace zero.core.runtime.scheduler
{
    public class IoZeroWorker
    {
        private readonly TaskScheduler _scheduler;

        public IoZeroWorker(TaskScheduler scheduler)
        {
            _scheduler = scheduler;
        }
        //public void Process(int j, Task prime = null)
        //{
        //    Task.Factory.StartNew(static async state =>
        //    {
        //        var (@this, workerId, prime) = (ValueTuple<ZeroWorker, int, Task>)state;

        //        //Prime task
        //        if (prime != null)
        //            @this._scheduler.TryExecuteTask(prime);

        //        while (!@this._asyncTasks.IsCancellationRequested)
        //        {
        //            try
        //            {
        //                if (@this._workQueue.Count == 0 &&
        //                    !await new ValueTask<bool>(@this._qUp[workerId], @this._qUp[workerId].Version))
        //                    break;

        //                Interlocked.Increment(ref @this._load);
        //                Task work;
        //                while ((work = await @this._workQueue.DequeueAsync().FastPath()) != null &&
        //                       work.Status == TaskStatus.WaitingToRun)
        //                    @this.TryExecuteTask(work);

        //                @this._qUp[workerId].Reset();
        //                Interlocked.Decrement(ref @this._load);
        //            }
        //            catch (Exception e)
        //            {
        //                LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoZeroScheduler)}");
        //            }
        //        }
        //    }, (this, j, prime), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        //}
    }
}
