﻿using System;
using System.Threading;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public struct IoManualResetValueStructTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
    {
        public IoManualResetValueStructTaskSource()
        {
            IIoManualResetValueTaskSourceCore<T> coreRef = new IoManualResetValueTaskSourceCore<T>();
            _coreRef = null;
            ZeroRef(ref coreRef);
        }

        public IoManualResetValueStructTaskSource(bool runContinuationsAsynchronously = false, bool runContinuationsNatively = true)
        {
            IIoManualResetValueTaskSourceCore<T> coreRef = new IoManualResetValueTaskSourceCore<T>
            {
                RunContinuationsAsynchronously = runContinuationsAsynchronously,
                RunContinuationsUnsafe = runContinuationsNatively
            };
            _coreRef = null;
            ZeroRef(ref coreRef);
        }

        private void ZeroRef(ref IIoManualResetValueTaskSourceCore<T> coreRef)
        {
            Interlocked.Exchange(ref _coreRef, coreRef);
        }

        public object Ref => _coreRef;
#if DEBUG
        private IIoManualResetValueTaskSourceCore<T> _coreRef;

        public bool RunContinuationsAsynchronously { get => _coreRef.RunContinuationsAsynchronously; set => _coreRef.RunContinuationsAsynchronously = value; }
        public void Reset() => _coreRef.Reset();
        public void SetResult(T result) => _coreRef.SetResult(result);
        public void SetException(Exception error) => _coreRef.SetException(error);

        public T GetResult(short token) => _coreRef.GetResult(token);
        void IValueTaskSource.GetResult(short token) => _coreRef.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _coreRef.GetStatus(token);
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _coreRef.OnCompleted(continuation, state, token, flags);
#else
        private IIoManualResetValueTaskSourceCore<T> _coreRef;

        public bool RunContinuationsAsynchronously { get => _coreRef.RunContinuationsAsynchronously; set => _coreRef.RunContinuationsAsynchronously = value; }
        public void Reset() => _coreRef.Reset();
        public void SetResult(T result) => _coreRef.SetResult(result);
        public void SetException(Exception error) => _coreRef.SetException(error);

        public T GetResult(short token) => _coreRef.GetResult(token);
        void IValueTaskSource.GetResult(short token) => _coreRef.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _coreRef.GetStatus(token);
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _coreRef.OnCompleted(continuation, state, token, flags);
#endif
        public bool IsBlocked(bool reset = false) => _coreRef.IsBlocking(reset);

    }
}
