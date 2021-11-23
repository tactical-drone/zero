using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public  class IoHashCodeEnum:IoEnumBase<int>
    {
        private IoHashCodes _c => (IoHashCodes)Collection;
        private volatile int _iteratorIdx = -1;
        private volatile int _iteratorCount;

        public IoHashCodeEnum(IoHashCodes codes):base(codes)
        {
            Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool MoveNext()
        {
            if (Disposed > 0)
                return false;

            int idx;
            while (_c[idx = Interlocked.Increment(ref _iteratorIdx) % _c.Capacity] == default && Interlocked.Decrement(ref _iteratorCount) > 0) { }
            return _c[idx] != default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, (_c.Tail - 1) % _c.Capacity);
            Interlocked.Exchange(ref _iteratorCount, _c.Count);
        }

        public override int Current => _c[_iteratorIdx % _c.Capacity];

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
