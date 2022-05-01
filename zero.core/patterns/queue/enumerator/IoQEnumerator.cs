using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public class IoQEnumerator<T>: IoEnumBase<T> where T : class
    {
        private IoZeroQ<T> ZeroQ => (IoZeroQ<T>)Collection;
        private long _cur = -1;
        //private long _iteratorCount;

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
        public override bool MoveNext() => Interlocked.Increment(ref _cur) < ZeroQ.Tail;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            Interlocked.Exchange(ref _cur, ZeroQ.Head - 1);
            //Interlocked.Exchange(ref _iteratorCount, ZeroQ.Count);
        }

        public override T Current => ZeroQ[_cur];

        public override void Dispose()
        {
            if (Disposed > 0 || Interlocked.CompareExchange(ref Disposed, 1, 0) != 0)
                return;

            Collection = null;
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public void IncIteratorCount()
        //{
        //    Interlocked.Increment(ref _iteratorCount);
        //}
    }
}
