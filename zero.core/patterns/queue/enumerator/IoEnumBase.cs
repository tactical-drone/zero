using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public abstract class IoEnumBase<T>: IEnumerator<T>
    {
        protected volatile IEnumerable<T> Collection;
        protected int Disposed = 0;
        protected private int InUse = 0;
        
        protected IoEnumBase(IEnumerable<T> collection)
        {
            Collection = collection;
        }

        public bool Zeroed => Disposed > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoEnumBase<T> Reuse(IEnumerable<T> container, Func<IEnumerable<T>, IoEnumBase<T>> make)
        {
            try
            {
                if (!Zeroed || Interlocked.CompareExchange(ref Disposed, 0, 1) == 0)
                    return make(container);

                Collection = container;
                return this;
            }
            finally
            {
                Reset();
            }
        }

        public abstract T Current { get; }

        object IEnumerator.Current => Current;

        public abstract void Dispose();
        public abstract bool MoveNext();
        public abstract void Reset();
    }
}
