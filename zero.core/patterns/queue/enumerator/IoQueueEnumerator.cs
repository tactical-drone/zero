using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public class IoQueueEnumerator<T> : IoEnumBase<IoQueue<T>.IoZNode>
    {
        private IoQueue<T> _q => (IoQueue<T>)Collection;
        private volatile IoQueue<T>.IoZNode _iteratorIoZNode;

        public IoQueueEnumerator(IoQueue<T> queue):base(queue)
        {
            Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool MoveNext()
        {
            if (Disposed > 0)
                return false;

            if (_q.Count == 0 || _iteratorIoZNode == null)
                return false;

            if (_q.Modified)
            {
                _q.Reset();
                return MoveNext();
            }

            _iteratorIoZNode = _iteratorIoZNode.Next;

            return _iteratorIoZNode != null && Disposed == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            _iteratorIoZNode = new IoQueue<T>.IoZNode { Next = _q.Head };
        }

        public override IoQueue<T>.IoZNode Current => _iteratorIoZNode;

        public volatile bool Modified;

        public override void Dispose()
        {
            if(Disposed > 0 || Interlocked.CompareExchange(ref Disposed, 1, 0) != 0)
                return;
            
            _iteratorIoZNode = null;
            Collection = null;
        }
    }
}
