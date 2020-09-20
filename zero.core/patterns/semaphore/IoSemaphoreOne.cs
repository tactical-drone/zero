using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public class IoSemaphoreOne<TMutex> : IoZeroable, IIoSemaphore
        where TMutex : class, IIoMutex, new()
    {
        public IoSemaphoreOne(int initialCount = 0, bool allowInliningAwaiters = true)
        {
#if DEBUG
            if (initialCount > 1 || initialCount < 0)
                throw new ArgumentException($"initial count = {initialCount} and max can only have the value 0 or 1");
#endif
            _mutex = ZeroOnCascade(new TMutex());
            Configure(initialCount, 1, false, allowInliningAwaiters);
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        protected override Task ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }


        public override string Description => $"{nameof(IoSemaphoreOne<TMutex>)}, mutex = {_mutex.Description}";

        /// <summary>
        /// The mutex we use to provide the semaphore
        /// </summary>
        private readonly IIoMutex _mutex;

        /// <summary>
        /// Configure the semaphore
        /// </summary>
        /// <param name="initialCount"></param>
        /// <param name="maxCapacity"></param>
        /// <param name="rangeCheck"></param>
        /// <param name="allowInliningContinuations"></param>
        /// <param name="token"></param>
        /// <param name="options"></param>s
        public void Configure(int initialCount = 0, int maxCapacity = 1, bool rangeCheck = false,
            bool allowInliningContinuations = true, TaskCreationOptions options = TaskCreationOptions.None)
        {
            _mutex.Configure(allowInliningContinuations);
            if (initialCount > 0)
                _mutex.SetAsync();
        }

        /// <summary>
        /// Set
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseAsync(int count = 1)
        {
            return _mutex.SetAsync();
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