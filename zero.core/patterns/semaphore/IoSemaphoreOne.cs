using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public class IoSemaphoreOne<TMutex> : IoNanoprobe, IIoSemaphore where TMutex : IIoMutex, new()
    {
        public IoSemaphoreOne(CancellationTokenSource asyncTasks, int initialCount = 0, bool allowInliningAwaiters = true) : base()
        {
#if DEBUG
            if (initialCount > 1 || initialCount < 0)
                throw new ArgumentException($"initial count = {initialCount} and max can only have the value 0 or 1");
#endif
            _mutex = new TMutex();
            if(initialCount == 1)
                _mutex.Set();
            
            Configure(asyncTasks, initialCount, 1, false, allowInliningAwaiters);
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }


        public override string Description => $"{nameof(IoSemaphoreOne<TMutex>)}, mutex = {_mutex}";

        /// <summary>
        /// The mutex we use to provide the semaphore
        /// </summary>
        private readonly TMutex _mutex;

        /// <summary>
        /// Configure the semaphore
        /// </summary>
        /// <param name="asyncTasks"></param>
        /// <param name="initialCount"></param>
        /// <param name="maxCapacity"></param>
        /// <param name="rangeCheck"></param>
        /// <param name="allowInliningContinuations"></param>
        /// <param name="options"></param>s
        public void Configure(CancellationTokenSource asyncTasks, int initialCount = 0, int maxCapacity = 1, bool rangeCheck = false,
            bool allowInliningContinuations = true, TaskCreationOptions options = TaskCreationOptions.None)
        {
            _mutex.Configure(asyncTasks, allowInliningContinuations);
            if (initialCount > 0)
                _mutex.Set();
        }

        /// <summary>
        /// Set
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(int count = 1)
        {
            _mutex.Set();
        }

        /// <summary>
        /// Wait
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            return _mutex.WaitAsync();
        }
    }
}