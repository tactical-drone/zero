using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// ManualResetValueTaskSource
    /// </summary>
    /// <typeparam name="T">The result type, can be anything</typeparam>
    public sealed class IoZeroResetValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
    {
        public IoZeroResetValueTaskSource(bool asyncInline = false)
        {
            _zeroCore.RunContinuationsAsynchronously = asyncInline;
        }

        private IoManualResetValueTaskSourceCore<T> _zeroCore;

        public bool RunContinuationsAsynchronously => _zeroCore.RunContinuationsAsynchronously;
        public int Version => _zeroCore.Version;
        public void Reset() => _zeroCore.Reset();
        public void SetResult(T result) => _zeroCore.SetResult(result);
        public void SetException(Exception exception) => _zeroCore.SetException(exception);
        public bool IsBlocked(bool reset = false) => _zeroCore.IsBlocking(reset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetResult(short token)
        {
            var result = _zeroCore.GetResult(token);
            _zeroCore.Reset();
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IValueTaskSource.GetResult(short token)
        {
            _zeroCore.GetResult(token);
            _zeroCore.Reset();
        }

        public ValueTaskSourceStatus GetStatus(short token) => _zeroCore.GetStatus(token);

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => GetStatus(token);
        
        ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => GetStatus(token);
        
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _zeroCore.OnCompleted(continuation, state, token, flags);

        public ValueTask<T> WaitAsync() => _zeroCore.GetStatus((short)_zeroCore.Version) != ValueTaskSourceStatus.Succeeded ? new ValueTask<T>(this, (short)_zeroCore.Version) : new ValueTask<T>(GetResult((short)Version));
        //public ValueTask<T> WaitAsync() => new ValueTask<T>(this, (short)_zeroCore.Version);
    }
}
