using System;
using System.Runtime.CompilerServices;
using System.Threading;
using NLog;

namespace zero.core.patterns.queue.enumerator
{
    public class IoQueueEnumerator<T> : IoEnumBase<IoQueue<T>.IoZNode>
    {
        private IoQueue<T> Q => (IoQueue<T>)Collection;
        private IoQueue<T>.IoZNode _iteratorIoZNode;

        public IoQueueEnumerator(IoQueue<T> queue):base(queue)
        {
            Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool MoveNext()
        {
            try
            {
                if (Q.Count == 0 || _iteratorIoZNode == null)
                    return false;

                if (Q.Modified)
                {
                    Q.Reset();
                    return MoveNext();
                }
                
                Interlocked.Exchange(ref _iteratorIoZNode, _iteratorIoZNode.Next);

                return _iteratorIoZNode != null;
            }
            catch when(Zeroed || Disposed > 0) {}
            catch (Exception e) when (!Zeroed && Disposed == 0)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{nameof(MoveNext)}:");
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Reset()
        {
            var node = new IoQueue<T>.IoZNode { Next = Q.Head };
            Interlocked.Exchange<IoQueue<T>.IoZNode>(ref _iteratorIoZNode, node);
        }

        public override IoQueue<T>.IoZNode Current => _iteratorIoZNode;

        public volatile bool Modified;

        public override void Dispose()
        {
            if(Disposed > 0 || Interlocked.CompareExchange(ref Disposed, 1, 0) != 0)
                return;

            Interlocked.Exchange(ref _iteratorIoZNode, null); 
            Collection = null;
        }
    }
}
