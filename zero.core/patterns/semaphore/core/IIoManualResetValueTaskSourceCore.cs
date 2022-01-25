using System;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoManualResetValueTaskSourceCore<T>: IValueTaskSource<T>
    {
        bool RunContinuationsAsynchronously { get; set; }
        short Version { get; }
        void Reset();
        void SetResult(T result);
        void SetException(Exception error);
    }
}
