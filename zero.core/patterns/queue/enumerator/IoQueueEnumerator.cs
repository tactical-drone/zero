using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.patterns.queue.enumerator
{
    public class IoQueueEnumerator<T> : IEnumerator<IoQueue<T>.IoZNode>
    {
        private IoQueue<T> _q;
        private volatile IoQueue<T>.IoZNode _iteratorIoZNode;
        private volatile int _disposed;

        public IoQueueEnumerator(IoQueue<T> queue)
        {
            _q = queue;
            _iteratorIoZNode = new IoQueue<T>.IoZNode { Next = _q.Head };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (_disposed > 0)
                return false;

            if (_q.Count == 0 || _iteratorIoZNode == null)
                return false;

            _iteratorIoZNode = _iteratorIoZNode.Next;

            return _iteratorIoZNode != null && _disposed == 0;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public IoQueue<T>.IoZNode Current => _iteratorIoZNode;

        object IEnumerator.Current => Current;

        public bool Modified;

        public void Dispose()
        {
            if(_disposed > 0 || Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;
            
            _iteratorIoZNode = null;
            _q = null;
        }
    }
}
