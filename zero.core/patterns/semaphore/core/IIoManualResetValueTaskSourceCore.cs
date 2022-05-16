using System;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoManualResetValueTaskSourceCore<TResult>: IValueTaskSource<TResult>
    {
        bool RunContinuationsAsynchronously { get; set; }
        bool AutoReset { get; }
        int Version { get; }
        void Reset();
        void Reset(short index);
        bool IsBlocking(bool reset);
        bool Primed { get; }
        bool Blocking { get; }
        bool Burned { get; }
        void SetResult(TResult result);
        //void SetResult(IIoManualResetValueTaskSourceCore<TResult> source);
        void SetException(Exception error);
    }
}
