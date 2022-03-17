using System;
using System.Runtime.CompilerServices;
using System.Threading;
using NLog;

namespace zero.core.patterns.queue.enumerator
{
    public class IoQueueEnumerator<T> : IoEnumBase<IoQueue<T>.IoZNode>
    {
        private IoQueue<T> Q => (IoQueue<T>)Collection;
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

            try
            {
                if (Q.Count == 0 || _iteratorIoZNode == null)
                    return false;

                if (Q.Modified)
                {
                    Q.Reset();
                    return MoveNext();
                }

                _iteratorIoZNode = _iteratorIoZNode.Next;

                return _iteratorIoZNode != null && Disposed == 0;
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
            try
            {
                _iteratorIoZNode = new IoQueue<T>.IoZNode { Next = Q.Head };
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
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
