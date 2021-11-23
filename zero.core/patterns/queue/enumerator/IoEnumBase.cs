using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public abstract class IoEnumBase<T>: IEnumerator<T>
    {
        protected IEnumerable<T> Collection;

        protected volatile int Disposed;

        protected IoEnumBase(IEnumerable<T> collection)
        {
            Collection = collection;
        }

        public bool Zeroed => Disposed > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoEnumBase<T> Reuse(IEnumerable<T> container, Func<IEnumerable<T>, IoEnumBase<T>> make)
        {
            if (Disposed == 0)
                return this;

            if (Interlocked.CompareExchange(ref Disposed, 0, 1) != 1)
                return make(container);

            Collection = container;
            return this;
        }

        public abstract T Current { get; }

        object IEnumerator.Current => Current;

        public abstract void Dispose();
        public abstract bool MoveNext();
        public abstract void Reset();
    }
}
