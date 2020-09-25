using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore
{
    /// <summary>
    /// Wraps a <see cref="IoZeroSemaphore"/> for use
    /// </summary>
    public class IoZeroNativeMutex:IIoMutex
    {
        public IoZeroNativeMutex(CancellationTokenSource asyncTasks, string description = "", int capacity = 1, int initialCount = 0, int expectedNrOfWaiters = 1,
            bool enableAutoScale = false, int zeroVersion = 0)
        {
            _mutex = new IoNativeMutex(asyncTasks);
        }

        private readonly IIoMutex _mutex;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetResult(short token)
        {
            return _mutex.GetResult(token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _mutex.GetStatus(token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _mutex.OnCompleted(continuation, state, token, flags);
        }

        public void Configure(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true)
        {
            _mutex.Configure(asyncTasks, signalled, allowInliningContinuations);
        }

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set()
        {
            _mutex.Set();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            return _mutex.WaitAsync();
        }

        public int GetWaited()
        {
            return _mutex.GetWaited();
        }

        public void SetWaited()
        {
            _mutex.SetWaited();
        }

        public int GetHooked()
        {
            return _mutex.GetHooked();
        }

        public void SetHooked()
        {
            _mutex.SetHooked();
        }

        public void SetResult(bool result)
        {
            _mutex.SetResult(result);
        }

        public void Zero()
        {
            _mutex.Zero();
        }

        public void ByRef(ref IIoMutex root)
        {
            _mutex.ByRef(ref root);
        }

        public short Version()
        {
            return _mutex.Version();
        }

        public ref IIoMutex GetRef(ref IIoMutex mutex)
        {
            return ref _mutex.GetRef(ref mutex);
        }

        public short GetCurFrame()
        {
            return _mutex.GetCurFrame();
        }

        public bool SetWaiter(Action<object> continuation, object state)
        {
            return _mutex.SetWaiter(continuation, state);
        }
    }
}