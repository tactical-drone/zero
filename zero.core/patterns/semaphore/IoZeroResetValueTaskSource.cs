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
    public sealed class IoZeroResetValueTaskSource<T> //: IValueTaskSource<T>, IValueTaskSource
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

        public bool RunContinuationsAsynchronously => _zeroCore.RunContinuationsAsynchronouslyAlways;
        public void SetResult(T result) => _zeroCore.SetResult(result);
        public void SetException(Exception exception) => _zeroCore.SetException(exception);
        
        public bool Blocking => _zeroCore.Blocking;
        public bool Primed => _zeroCore.Primed;
        public bool Burned => _zeroCore.Burned;
        public T GetResult(short token) => _zeroCore.GetResult(token);

        public ValueTaskSourceStatus GetStatus(short token) => _zeroCore.GetStatus(token);

        public ValueTask<T> WaitAsync() => new(_zeroCore, (short)_zeroCore.Version);

        public override string ToString() => _zeroCore.ToString();

        public void Reset() => _zeroCore.Reset();

    }
}
