using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public  class IoHashCodeEnum:IoEnumBase<int>
    {
        private IoHashCodes HashCodes => (IoHashCodes)Collection;
        private volatile int _iteratorIdx = -1;
        private volatile int _iteratorCount;

        public IoHashCodeEnum(IoHashCodes codes):base(codes)
        {
            Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool MoveNext()
        {
            if (Disposed > 0 || HashCodes.Zeroed || _iteratorCount <= 0)
                return false;

            var idx = Interlocked.Increment(ref _iteratorIdx) % HashCodes.Capacity;

            while (Interlocked.Decrement(ref _iteratorCount) > 0 && HashCodes[idx] == default)
                idx = Interlocked.Increment(ref _iteratorIdx) % HashCodes.Capacity;

            return _iteratorCount >= 0 && HashCodes[idx] != default && Disposed == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, (HashCodes.Tail - 1) % HashCodes.Capacity);
            Interlocked.Exchange(ref _iteratorCount, HashCodes.Count);
        }

        public override int Current => HashCodes[_iteratorIdx % HashCodes.Capacity];

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
