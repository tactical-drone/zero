using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public  class IoHashCodeEnum:IEnumerator<int>
    {
        private IoHashCodes _c;
        private volatile int _iteratorIdx = -1;
        private volatile int _iteratorCount;
        private int _disposed;

        public IoHashCodeEnum(IoHashCodes codes)
        {
            _c = codes;
            Interlocked.Exchange(ref _iteratorIdx, (_c.Tail - 1) % _c.Capacity);
            Interlocked.Exchange(ref _iteratorCount, _c.Count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (_disposed > 0)
                return false;

            int idx;
            while (_c[idx = Interlocked.Increment(ref _iteratorIdx) % _c.Capacity] == default && Interlocked.Decrement(ref _iteratorCount) > 0) { }
            return _c[idx] != default;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public int Current => _c[_iteratorIdx % _c.Capacity];

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (_disposed > 0 || Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;

            _c = null;
        }
    }
}
