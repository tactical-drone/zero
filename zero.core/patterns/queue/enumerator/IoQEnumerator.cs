using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public class IoQEnumerator<T>: IoEnumBase<T> where T : class
    {
        private IoZeroQ<T> ZeroQ => (IoZeroQ<T>)Collection;
        private long _iteratorIdx = -1;
        private long _iteratorCount;
        

        public IoQEnumerator(IoZeroQ<T> zeroQ):base(zeroQ)
        {
            Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoQEnumerator<T> Reuse(IoZeroQ<T> zeroQ)
        {
            return Interlocked.CompareExchange(ref Disposed, 0, 1) != 1 ? new IoQEnumerator<T>(zeroQ) : this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool MoveNext()
        {
            if (Disposed > 0 || ZeroQ.Zeroed || _iteratorCount <= 0)
                return false;

            var idx = Interlocked.Increment(ref _iteratorIdx) % ZeroQ.Capacity;

            while (Interlocked.Decrement(ref _iteratorCount) > 0 && ZeroQ[(int)idx] == default)
                idx = Interlocked.Increment(ref _iteratorIdx) % ZeroQ.Capacity;

            return _iteratorCount >= 0 && ZeroQ[(int)idx] != default && Disposed == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, (ZeroQ.Head - 1) % ZeroQ.Capacity);
            Interlocked.Exchange(ref _iteratorCount, ZeroQ.Count);
        }

        public override T Current => ZeroQ[(int)(_iteratorIdx % ZeroQ.Capacity)];

        public override void Dispose()
        {
            if (Disposed > 0 || Interlocked.CompareExchange(ref Disposed, 1, 0) != 0)
                return;

            Collection = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncIteratorCount()
        {
            Interlocked.Increment(ref _iteratorCount);
        }
    }
}
