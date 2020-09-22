using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public class ZeroCompletionSource<T> : TaskCompletionSource<T>, IIoNanoprobe
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
            _nanoprobe = new IoNanoprobe();
        }

        private readonly IIoNanoprobe _nanoprobe;

        public IIoZeroable ZeroedFrom => _nanoprobe.ZeroedFrom;

        public ulong NpId => _nanoprobe.NpId;

        /// <summary>
        /// Description 
        /// </summary>
        public virtual string Description => $"{nameof(ZeroCompletionSource<T>)}";
        
        /// <summary>
        /// zero unmanaged
        /// </summary>
        /// <param name="from"></param>
        /// <returns></returns>
        public ValueTask ZeroAsync(IIoZeroable from)
        {
            TrySetCanceled(_nanoprobe.AsyncTokenProxy.Token);
            return _nanoprobe.ZeroAsync(from);
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <param name="sub"></param>
        /// <returns></returns>
        public IoZeroSub ZeroEvent(Func<IIoZeroable, Task> sub)
        {
            return _nanoprobe.ZeroEvent(sub);
        }

        public void Unsubscribe(IoZeroSub sub)
        {
            _nanoprobe.Unsubscribe(sub);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (TBase target, bool success) ZeroOnCascade<TBase>(TBase target, bool twoWay = false) where TBase : IIoZeroable
        {
            return ((IIoZeroable) _nanoprobe).ZeroOnCascade(target, twoWay);
        }

        public bool Zeroed()
        {
            return _nanoprobe.Zeroed();
        }

        public ValueTask<bool> ZeroEnsureAsync(Func<IIoZeroable, Task<bool>> ownershipAction, bool force = false)
        {
            return _nanoprobe.ZeroEnsureAsync(ownershipAction, force);
        }

        public void ZeroUnmanaged()
        {
            _nanoprobe.ZeroUnmanaged();
        }

        public ValueTask ZeroManagedAsync()
        {
            return _nanoprobe.ZeroManagedAsync();
        }

        private static TaskCreationOptions AdjustFlags(TaskCreationOptions options, bool allowInliningContinuations)
        {
            return allowInliningContinuations
                ? (options & ~TaskCreationOptions.RunContinuationsAsynchronously)
                : (options | TaskCreationOptions.RunContinuationsAsynchronously);
        }
        public CancellationTokenSource AsyncTokenProxy => _nanoprobe.AsyncTokenProxy;
        
        public bool Equals(IIoZeroable other)
        {
            throw new NotImplementedException();
        }
    }
}