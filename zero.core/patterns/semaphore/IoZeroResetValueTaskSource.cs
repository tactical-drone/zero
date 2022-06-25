using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// ManualResetValueTaskSource
    /// </summary>
    /// <typeparam name="T">The result type, can be anything</typeparam>
    public sealed class IoZeroResetValueTaskSource<T> : IIoManualResetValueTaskSourceCore<T> // IValueTaskSource<T>, IValueTaskSource
    {
        public IoZeroResetValueTaskSource(bool runContinuationsAsynchronously = false)
        {
            _zeroCore = new IoManualResetValueTaskSourceCore<T>
            {
                RunContinuationsAsynchronouslyAlways = runContinuationsAsynchronously,
                AutoReset = true
            };
        }

        private readonly IIoManualResetValueTaskSourceCore<T> _zeroCore;

        public bool RunContinuationsAsynchronously
        {
            get => _zeroCore.RunContinuationsAsynchronouslyAlways;
            set => _zeroCore.RunContinuationsAsynchronouslyAlways = value;
        }

        public bool RunContinuationsAsynchronouslyAlways { get; set; }
        public bool AutoReset => _zeroCore.AutoReset;
        public int Version => _zeroCore.Version;

        public void SetResult(T result) => _zeroCore.SetResult(result);
        public void SetException(Exception exception) => _zeroCore.SetException(exception);
        public void Reset(Action<object> index, object context)
        {
            throw new NotImplementedException();
        }

        public bool Blocking => _zeroCore.Blocking;
        public bool IsBlocking(bool reset)
        {
            throw new NotImplementedException();
        }

        public bool Primed => _zeroCore.Primed;
#if DEBUG
        public bool Burned => _zeroCore.Burned;
#endif
        public int Relay
        {
            get => _zeroCore.Relay;
            set => _zeroCore.Relay = value;
        }

        public T GetResult(short token) => _zeroCore.GetResult(token);

        public ValueTaskSourceStatus GetStatus(short token) => _zeroCore.GetStatus(token);
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) => _zeroCore.OnCompleted(continuation, state, token, flags);

        public ValueTask<T> WaitAsync() => new(_zeroCore, (short)_zeroCore.Version);

        public override string ToString() => _zeroCore.ToString();

        public void Reset() => _zeroCore.Reset();
        public void Reset(int version) => _zeroCore.Reset(version);

    }
}
