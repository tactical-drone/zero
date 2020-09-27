using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.conf;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.misc
{
    public class IoNanoprobe: IoConfigurable, IIoNanoprobe
    {
        public IoNanoprobe()
        {
            _nanoprobe = new IoZeroable<IoNativeMutex>(AsyncTokenProxy);
        }
        private IoZeroable<IoNativeMutex> _nanoprobe;
        public IoZeroable<IoNativeMutex> Nanoprobe => _nanoprobe;
        public CancellationTokenSource AsyncTokenProxy { get; } = new CancellationTokenSource();
        public ulong NpId => _nanoprobe.NpId;
        
        public virtual string Description => $"{nameof(IoNanoprobe)}";
        public IIoZeroable ZeroedFrom => _nanoprobe.ZeroedFrom;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ZeroAsync(IIoZeroable from)
        {
            AsyncTokenProxy.Cancel();//TODO is the the right spot
            return _nanoprobe.ZeroAsync(from);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (TBase target, bool success) ZeroOnCascade<TBase>(TBase target, bool twoWay = false) where TBase : IIoZeroable
        {
            return _nanoprobe.ZeroOnCascade(target, twoWay);
        }

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoZeroSub ZeroEvent(Func<IIoZeroable, Task> sub)
        {
            return _nanoprobe.ZeroEvent(sub);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unsubscribe(IoZeroSub sub)
        {
            _nanoprobe.Unsubscribe(sub);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Zeroed()
        {
            return _nanoprobe.Zeroed();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ZeroEnsureAsync(Func<IIoZeroable, Task<bool>> ownershipAction, bool force = false)
        {
            return _nanoprobe.ZeroEnsureAsync(ownershipAction, force);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void ZeroUnmanaged()
        {
            _nanoprobe.ZeroUnmanaged();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ValueTask ZeroManagedAsync()
        {
            return _nanoprobe.ZeroManagedAsync();
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(IIoZeroable other)
        {
            return other.NpId == NpId;
        }
    }
}