using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public class IoBagEnumerator<T>: IoEnumBase<T> where T : class
    {
        private IoBag<T> Bag => (IoBag<T>)Collection;
        private long _iteratorIdx = -1;
        private long _iteratorCount;
        

        public IoBagEnumerator(IoBag<T> bag):base(bag)
        {
            Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoBagEnumerator<T> Reuse(IoBag<T> bag)
        {
            return Interlocked.CompareExchange(ref Disposed, 0, 1) != 1 ? new IoBagEnumerator<T>(bag) : this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool MoveNext()
        {
            if (Disposed > 0 || Bag.Zeroed || _iteratorCount <= 0)
                return false;

            var idx = Interlocked.Increment(ref _iteratorIdx) % Bag.Capacity;

            while (Interlocked.Decrement(ref _iteratorCount) > 0 && Bag[(int)idx] == default)
                idx = Interlocked.Increment(ref _iteratorIdx) % Bag.Capacity;

            return _iteratorCount >= 0 && Bag[(int)idx] != default && Disposed == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            Interlocked.Exchange(ref _iteratorIdx, (Bag.Head - 1) % Bag.Capacity);
            Interlocked.Exchange(ref _iteratorCount, Bag.Count);
        }

        public override T Current => Bag[(int)(_iteratorIdx % Bag.Capacity)];

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
