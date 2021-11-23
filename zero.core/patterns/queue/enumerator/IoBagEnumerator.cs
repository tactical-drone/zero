using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public class IoBagEnumerator<T>: IEnumerator<T> where T : class
    {
        private IoBag<T> _b;
        private int _iteratorIdx = -1;
        private int _iteratorCount;
        private int _disposed;

        public IoBagEnumerator(IoBag<T> bag)
        {
            _b = bag;
            Interlocked.Exchange(ref _iteratorIdx, (_b.Tail - 1) % _b.Capacity);
            Interlocked.Exchange(ref _iteratorCount, _b.Count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (_disposed > 0 || _b.Zeroed || _iteratorCount <= 0)
                return false;

            var idx = Interlocked.Increment(ref _iteratorIdx) % _b.Capacity;

            while (Interlocked.Decrement(ref _iteratorCount) > 0 && _b[idx] == default)
                idx = Interlocked.Increment(ref _iteratorIdx) % _b.Capacity;

            return _iteratorCount >= 0 && _b[idx] != default && _disposed == 0;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public T Current => _b[_iteratorIdx % _b.Capacity];

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (_disposed > 0 || Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;

            _b = null;
        }
    }
}
