using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public interface IIoMutex : IValueTaskSource<bool>
    {
        void Configure( CancellationTokenSource asyncTasks, bool signalled = false,
            bool allowInliningContinuations = true);
        void Set();
        ValueTask<bool> WaitAsync();
        int GetWaited();
        void SetWaited();
        int GetHooked();
        void SetHooked();
        void SetResult(bool result);

        /// <summary>
        /// Resets for next use
        /// </summary>
        void Zero();

        void ByRef(ref IIoMutex root);
        short Version();

        ref IIoMutex GetRef(ref IIoMutex mutex);
        short GetCurFrame();
        bool SetWaiter(Action<object> continuation, object state);
    }
}