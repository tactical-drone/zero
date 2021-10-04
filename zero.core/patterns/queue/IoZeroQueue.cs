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
        public IoZeroQueue(string description, int capacity, int concurrencyLevel, bool enableBackPressure = false)
        {
            _description = description;
            
            _nodeHeap = new IoHeap<IoZNode>(capacity){Make = o => new IoZNode()};
            
            _syncRoot = new IoZeroSemaphoreSlim(_asyncTasks.Token, description,
                maxBlockers: concurrencyLevel*2, maxAsyncWork:0, initialCount: 1);

            _pressure = new IoZeroSemaphoreSlim(_asyncTasks.Token, $"q pressure at {description}",
                maxBlockers: concurrencyLevel*2, maxAsyncWork:0, initialCount: 0);

            _enableBackPressure = enableBackPressure;
            if(_enableBackPressure)
                _backPressure = new IoZeroSemaphoreSlim(_asyncTasks.Token, $"q back pressure at {description}",
                    maxBlockers: concurrencyLevel*2,maxAsyncWork:0, initialCount: 1);
        }

        private readonly string _description; 
        private volatile bool _zeroed;
        private IoZeroSemaphoreSlim _syncRoot;
        private IoZeroSemaphoreSlim _pressure;
        private IoZeroSemaphoreSlim _backPressure;
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();
        private IoHeap<IoZNode> _nodeHeap;

        private volatile IoZNode _head = null;
        private volatile IoZNode _tail = null;
        private volatile int _count;
        private readonly bool _enableBackPressure;
        public int Count => _count;
        public IoZNode First => _head;
        public IoZNode Last => _tail;

        public async ValueTask ZeroManagedAsync<TC>(Func<T,TC, ValueTask> op = null, TC nanite = default)
        {
            try
            {
                if (_zeroed || !await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false))
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
                            await op(cur.Value, nanite).FastPath().ConfigureAwait(false);
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

                await _nodeHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
                _nodeHeap = null;

                _asyncTasks.Dispose();
                _asyncTasks = null;

                var from = new IoNanoprobe($"{_description}");
                await _pressure.ZeroAsync(from).ConfigureAwait(false);

                if(_enableBackPressure)
                    await _backPressure.ZeroAsync(from).ConfigureAwait(false);

                //unmanaged
                _pressure = null;
                _backPressure = null;

                //zeroed
                _zeroed = true;
            }
            finally
            {
                _syncRoot.Release();
            }

            await _syncRoot.ZeroAsync(new IoNanoprobe($"{_description}")).FastPath().ConfigureAwait(false);
            _syncRoot = null;
        }

        /// <summary>
        /// Blocking enqueue item
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The enqueued item node</returns>
        public async ValueTask<IoZNode> EnqueueAsync(T item)
        {
            try
            {
                if (_zeroed || item == null)
                    return null;

                //wait on back pressure
                if (_enableBackPressure && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(false))
                        return null;

                _nodeHeap.Take(out var node);

                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.CurrentHeapSize} + {_nodeHeap.ReferenceCount})/{_nodeHeap.MaxSize}, count = {_count}");

                //set value
                node.Value = item;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed)
                {
                    await _nodeHeap.ReturnAsync(node).FastPath().ConfigureAwait(false); ;
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
                _pressure.Release();
            }
        }

        /// <summary>
        /// Blocking dequeue item
        /// </summary>
        /// <returns>The dequeued item</returns>
        public async ValueTask<T> DequeueAsync()
        {
            IoZNode dq = null;
            try
            {
                if (_zeroed || _count == 0)
                    return default;

                if (!await _pressure.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed)
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
                if(Interlocked.Decrement(ref _count) > 0)
                    _tail = p;
                else
                    _head = _tail = null;
            }
            finally
            {
                _syncRoot.Release();
            }

            //return dequeued item
            if (dq != null)
            {
                try
                {
                    dq.Prev = null;
                    var retVal = dq.Value;
                    await _nodeHeap.ReturnAsync(dq).FastPath().ConfigureAwait(false); ;
                    return retVal;
                }
                finally
                {
                    if(_enableBackPressure)
                        _backPressure.Release();
                }
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
                if(_zeroed || node == null)
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
                    _head = node.Next;

                if (_tail == node)
                    _tail = node.Prev;

                Interlocked.Decrement(ref _count);
            }
            finally
            {
                _syncRoot.Release();
                node!.Prev = null;
                node!.Next = null;
            }

            await _nodeHeap.ReturnAsync(node).FastPath().ConfigureAwait(false);
        }
    }
}
