using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public class IoManualResetValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
    {
        public IoManualResetValueTaskSource()
        {
            IIoManualResetValueTaskSourceCore <T> coreRef = new IoManualResetValueTaskSourceCore<T>();
            ZeroRef(ref coreRef);
        }

        public IoManualResetValueTaskSource(bool runContinuationsAsynchronously = false, bool runContinuationsNatively = true)
        {
            IIoManualResetValueTaskSourceCore<T> coreRef = new IoManualResetValueTaskSourceCore<T>
            {
                RunContinuationsAsynchronously = runContinuationsAsynchronously,
                RunContinuationsUnsafe = runContinuationsNatively
            }; 
            ZeroRef(ref coreRef);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroRef(ref IIoManualResetValueTaskSourceCore<T> coreRef)
        {
            Interlocked.Exchange(ref _coreRef, coreRef);
        }

        public object Ref => _coreRef;
#if DEBUG
        private IIoManualResetValueTaskSourceCore<T> _coreRef;

        public bool RunContinuationsAsynchronously { get => _coreRef.RunContinuationsAsynchronously; set => _coreRef.RunContinuationsAsynchronously = value; }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset() => _coreRef.Reset();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetResult(T result) => _coreRef.SetResult(result);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetException(Exception error) => _coreRef.SetException(error);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        public T GetResult(short token) => _coreRef.GetResult(token);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IValueTaskSource.GetResult(short token) => _coreRef.GetResult(token);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token = 0) => _coreRef.GetStatus(0);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _coreRef.OnCompleted(continuation, state, token, flags);
#else
        private IIoManualResetValueTaskSourceCore<T> _coreRef;

        public bool RunContinuationsAsynchronously { get => _coreRef.RunContinuationsAsynchronously; set => _coreRef.RunContinuationsAsynchronously = value; }
        public void Reset() => _coreRef.Reset();
        public void SetResult(T result) => _coreRef.SetResult(result);
        public void SetException(Exception error) => _coreRef.SetException(error);

        public T GetResult(short token) => _coreRef.GetResult(token);
        void IValueTaskSource.GetResult(short token) => _coreRef.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token = 0) => _coreRef.GetStatus(0);
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _coreRef.OnCompleted(continuation, state, token, flags);
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsBlocked(bool reset = false) => _coreRef.IsBlocking(reset);

    }
}
