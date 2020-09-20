using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public class ZeroCompletionSource<T> : TaskCompletionSource<T>, IIoZeroable
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="allowInliningContinuations"></param>
        /// <param name="options"></param>
        public ZeroCompletionSource(bool allowInliningContinuations = true,
            TaskCreationOptions options = TaskCreationOptions.None)
            : base(null, AdjustFlags(options, allowInliningContinuations))
        {
        }

        /// <summary>
        /// Implements <see cref="IIoZeroable"/>
        /// </summary>
        private readonly IoZeroable _zeroComposition = new IoZeroable();
        
        /// <summary>
        /// Description 
        /// </summary>
        public virtual string Description => $"{nameof(ZeroCompletionSource<T>)}";
        
        public IIoZeroable ZeroedFrom => _zeroComposition.ZeroedFrom;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        /// <param name="from"></param>
        /// <returns></returns>
        public Task ZeroAsync(IIoZeroable from)
        {
            TrySetCanceled(_zeroComposition.AsyncTasks.Token);
            return _zeroComposition.ZeroAsync(from);
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <param name="sub"></param>
        /// <returns></returns>
        public IoZeroable.ZeroSub ZeroEvent(Func<IIoZeroable, Task> sub)
        {
            return _zeroComposition.ZeroEvent(sub);
        }

        public void Unsubscribe(IoZeroable.ZeroSub sub)
        {
            _zeroComposition.Unsubscribe(sub);
        }

        public T1 ZeroOnCascade<T1>(T1 target, bool twoWay = false) where T1 : class, IIoZeroable
        {
            return _zeroComposition.ZeroOnCascade(target, twoWay);
        }

        public bool Zeroed()
        {
            return _zeroComposition.Zeroed();
        }

        public Task<bool> ZeroEnsureAsync(Func<Task<bool>> ownershipAction, bool force = false)
        {
            return _zeroComposition.ZeroEnsureAsync(ownershipAction, force);
        }

        private static TaskCreationOptions AdjustFlags(TaskCreationOptions options, bool allowInliningContinuations)
        {
            return allowInliningContinuations
                ? (options & ~TaskCreationOptions.RunContinuationsAsynchronously)
                : (options | TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}