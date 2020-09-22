using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public interface IIoSemaphore 
    {
        /// <summary>
        /// Configure the semaphore
        /// </summary>
        /// <param name="asyncTasks"></param>
        /// <param name="initialCount">initial waiters skipped</param>
        /// <param name="maxCapacity">maximum capacity</param>
        /// <param name="rangeCheck">Range check the semaphore on use</param>
        /// <param name="allowInliningContinuations">allow sync continuations</param>
        /// <param name="options">Continuation options</param>
        void Configure(CancellationTokenSource asyncTasks, int initialCount = 0, int maxCapacity = 1, bool rangeCheck = false,
            bool allowInliningContinuations = true, TaskCreationOptions options = TaskCreationOptions.None);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        void Release(int count = 1);

        /// <summary>
        /// Blocks if there are no slots available
        /// </summary>
        /// <returns>True if success, false otherwise</returns>
        ValueTask<bool> WaitAsync();
    }
}