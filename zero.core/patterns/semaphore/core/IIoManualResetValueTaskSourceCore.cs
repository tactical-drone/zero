using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoManualResetValueTaskSourceCore<TResult>: IValueTaskSource<TResult>
    {
        bool RunContinuationsAsynchronously { get; set; }
        bool RunContinuationsAsynchronouslyAlways { get; set; }
        bool AutoReset { get; }
        int Version { get; }
        void Reset();
        void Reset(int version);
        bool IsBlocking(bool reset);
        bool Primed { get; }
        bool Blocking { get; }
#if DEBUG
        bool Burned { get; }
#endif
        int Relay { get; set; }

        void SetResult(TResult result);
        void SetException(Exception error);
        void Reset(Action<object> index, object context);
        ValueTask<TResult> WaitAsync();
    }
}
