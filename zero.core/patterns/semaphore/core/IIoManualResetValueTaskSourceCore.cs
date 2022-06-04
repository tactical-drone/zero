using System;
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
        void Prime(int version);
        bool IsBlocking(bool reset);
        bool Primed { get; }
        bool Blocking { get; }
        bool Burned { get; }
        int Relay { get; set; }

        bool Lock();
        IIoManualResetValueTaskSourceCore<TResult> Free();

        //object BurnContext { get; set; }
        //void SetResult<TContext>(TResult result, Action<bool, TContext> async = null, TContext context = default);
        void SetResult(TResult result);
        //void SetResult(IIoManualResetValueTaskSourceCore<TResult> source);
        void SetException(Exception error);
        void Reset(Action<object> index, object context);
    }
}
