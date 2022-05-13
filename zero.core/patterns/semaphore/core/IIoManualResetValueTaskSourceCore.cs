using System;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoManualResetValueTaskSourceCore<TResult>: IValueTaskSource<TResult>
    {
        bool RunContinuationsAsynchronously { get; set; }
        int Version { get; }
        void Reset();
        void Reset(short index);
        bool Set(bool reset);
        void SetResult(TResult result);
        void SetException(Exception error);
    }
}
