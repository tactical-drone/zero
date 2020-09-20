using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.semaphore
{
    public class IoAutoMutex : IoZeroable, IIoMutex
    {
        /// <summary>
        /// ctor
        /// </summary>
        public IoAutoMutex()
        :base(true)
        {
            Configure();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="signalled"></param>
        /// <param name="allowInliningContinuations"></param>
        public IoAutoMutex(bool signalled = false,  bool allowInliningContinuations = true)
        :base(true)
        {
            Configure(signalled, allowInliningContinuations);
        }
        public override string Description => $"{nameof(IoAutoMutex)}";

        private ConcurrentQueue<ZeroCompletionSource<bool>> _signalAwaiters =
            new ConcurrentQueue<ZeroCompletionSource<bool>>();

        private volatile int _signalled;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _signalAwaiters = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        protected override Task ZeroManagedAsync()
        {
            //Unblock all blockerss
            foreach (var zeroCompletionSource in _signalAwaiters)
                zeroCompletionSource.ZeroAsync(this).ConfigureAwait(false);

            _signalAwaiters.Clear();

            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// Configure
        /// </summary>
        /// <param name="signalled">Initial state</param>
        /// <param name="allowInliningContinuations"></param>
        public void Configure(bool signalled = false, bool allowInliningContinuations = true)
        {
            _csHeap = ZeroOnCascade(new IoHeap<ZeroCompletionSource<bool>>(10, this)
                {Make = o => ((IoAutoMutex)o).ZeroOnCascade(new ZeroCompletionSource<bool>(allowInliningContinuations))});

            _signalled = signalled ? 1 : 0;
        }

        /// <summary>
        /// Signal
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask SetAsync()
        {
            //get a waiter
            if (_signalAwaiters.TryDequeue(out var toRelease))
            {
                toRelease.TrySetResult(!AsyncTasks.IsCancellationRequested);
                //await _csHeap.ReturnAsync(toRelease).ConfigureAwait(false);
                await toRelease.ZeroAsync(this);
            }
            else
                Interlocked.Exchange(ref _signalled, 1);
        }

        private static readonly ValueTask<bool> FalseResult = new ValueTask<bool>(false);
        private static readonly ValueTask<bool> TrueResult = new ValueTask<bool>(true);
        private IoHeap<ZeroCompletionSource<bool>> _csHeap;
        
        /// <summary>
        /// Wait
        /// </summary>
        /// <returns></returns>
        /// <exception cref="OutOfMemoryException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> WaitAsync()
        {
            //fail fast
            if (Zeroed())
                return FalseResult;

            //were we signalled?
            ZeroCompletionSource<bool> waiter;
            if (Interlocked.CompareExchange(ref _signalled, 0, 1) == 1)
            {
                return TrueResult;
            }
            else
            {
                // var takeTask = _csHeap.TakeAsync(this);
                // await takeTask.OverBoostAsync().ConfigureAwait(false);
                // waiter = takeTask.Result;
                waiter = ZeroOnCascade(new ZeroCompletionSource<bool>(true));

#if DEBUG
                if (waiter == null)
                    throw new OutOfMemoryException(
                        $"{nameof(IoHeap<ZeroCompletionSource<bool>>)}: Heap depleted: taken = {_signalAwaiters.Count}, max = {_csHeap.MaxSize}");
#else
                if (waiter == null)
                {
                    ZeroAsync(this).ConfigureAwait(false);
                    return FalseResult;
                }
#endif

                if (Zeroed())
                    waiter.TrySetCanceled(AsyncTasks.Token);
                else
                    _signalAwaiters.Enqueue(waiter);
            }

            return new ValueTask<bool>(waiter.Task);
        }

        /// <summary>
        /// Not applicable
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Reset()
        {
            throw new NotImplementedException();
        }
    }
}