using System;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.queue
{

    /// <summary>
    /// Zero Queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class IoZeroQueue<T>
    {
        /// <summary>
        /// A node
        /// </summary>
        public class IoZNode
        {
            public T Value;
            public volatile IoZNode Next;
            public volatile IoZNode Prev;
        }
        /// <summary>
        /// constructor
        /// </summary>
        public IoZeroQueue(string description, int heapSize = 16)
        {
            _description = description;
            _nodeHeap = new IoHeap<IoZNode>(heapSize){Make = o => new IoZNode()};
            _syncRoot = new IoZeroSemaphoreSlim(_asyncTasks.Token, description, initialCount: 1);
        }

        private readonly string _description; 
        private volatile bool _zeroed;
        private IoZeroSemaphoreSlim _syncRoot;
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();
        private IoHeap<IoZNode> _nodeHeap;

        private volatile IoZNode _head = null;
        private volatile IoZNode _tail = null;
        private volatile int _count;
        public int Count => _count;
        public IoZNode First => _head;
        public IoZNode Last => _tail;

        public async ValueTask ZeroManagedAsync(Func<T, ValueTask> op = null)
        {
            try
            {
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed)
                    return;
                
                if(!_asyncTasks.IsCancellationRequested)
                    _asyncTasks.Cancel();

                if (op != null)
                {
                    var cur = First;
                    while (cur != null)
                    {

                        try
                        {
                            await op(cur.Value).FastPath().ConfigureAwait(false);
                        }
                        catch 
                        {
                            //
                        }

                        cur = cur.Next;
                    }
                }

                _head = null;
                _tail = null;

                await _nodeHeap.ZeroManaged().FastPath().ConfigureAwait(false);
                _nodeHeap = null;

                _asyncTasks.Dispose();
                _asyncTasks = null;
                _zeroed = true;
            }
            finally
            {
                _syncRoot.Release();
            }

            _syncRoot.Zero();
            _syncRoot = null;
        }

        /// <summary>
        /// Enqueue item
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The enqueued item node</returns>
        public async ValueTask<IoZNode> EnqueueAsync(T item)
        {
            try
            {
                _nodeHeap.Take(out var node);

                if (node == null)
                    throw new OutOfMemoryException($"{_description}");

                //set value
                node.Value = item;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed)
                {
                    _nodeHeap.Return(node);
                    return null;
                }
                
                //set hooks
                node.Next = _head;

                //plumbing
                if (_head == null)
                {
                    _head = _tail = node;
                }
                else //hook
                {
                    node.Prev = _head.Prev;
                    _head.Prev = node;
                    _head = node;
                }
                
                Interlocked.Increment(ref _count);

                return node;
            }
            finally
            {
                _syncRoot.Release();
            }
        }

        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public async ValueTask<T> DequeueAsync()
        {
            IoZNode dq = null;
            try
            {
                if (_count == 0)
                    return default;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed)
                    return default;

                //fail fast
                if (_tail == null)
                    return default;

                //un-hook
                dq = _tail;
                var p = _tail.Prev;
                if (p != null)
                    p.Next = null;

                //plumbing
                if (_head == _tail)
                    _head = _tail = null;
                else
                    _tail = p;

                Interlocked.Decrement(ref _count);
            }
            finally
            {
                _syncRoot.Release();
            }

            //return dequeued item
            if (dq != null)
            {
                dq.Prev = null;
                var retVal = dq.Value;
                _nodeHeap.Return(dq);
                return retVal;
            }

            return default;
        }

        /// <summary>
        /// Removes a node from the queue
        /// </summary>
        /// <param name="node">The node to remove</param>
        /// <returns>Value task</returns>
        public async ValueTask RemoveAsync(IoZNode node)
        {
            try
            {
                if(node == null)
                    return;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed)
                    return;

                //unhook
                if (node.Prev != null)
                {
                    var next = node.Next;
                    node.Prev.Next = next;

                    if (next != null)
                        next.Prev = node.Prev;

                }
                
                //plumbing
                if (_head == node)
                    _head = _head.Next;

                if (_tail == node)
                    _tail = _tail.Prev;

                Interlocked.Decrement(ref _count);
            }
            finally
            {
                _syncRoot.Release();
            }
        }
    }
}
