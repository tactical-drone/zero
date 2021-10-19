using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.queue
{

    /// <summary>
    /// Zero Queue
    /// </summary>
    /// <typeparam name="T">The type of item queued</typeparam>
    public class IoQueue<T>: IEnumerator<IoQueue<T>.IoZNode>, IEnumerable<IoQueue<T>.IoZNode>
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
        public IoQueue(string description, uint capacity, int concurrencyLevel, bool enableBackPressure = false, bool disablePressure = true)
        {
            _description = description;

            _nodeHeap = new IoHeap<IoZNode>(capacity){Make = static (o,s) => new IoZNode()};
            
            _syncRoot = new IoZeroSemaphore($"{nameof(_syncRoot)} {description}",
                maxBlockers: concurrencyLevel, initialCount: 1);
            _syncRoot.ZeroRef(ref _syncRoot, _asyncTasks.Token);

            //_syncRoot = new IoZeroRefMut(_asyncTasks.Token);

            if (!disablePressure)
            {
                _pressure = new IoZeroSemaphore($"qp {description}",
                    maxBlockers: concurrencyLevel, maxAsyncWork: 0, initialCount: 0);
                _pressure.ZeroRef(ref _pressure, _asyncTasks.Token);
            }
            
            _enableBackPressure = enableBackPressure;
            if (_enableBackPressure)
            {
                _backPressure = new IoZeroSemaphore($"qbp {description}",
                    maxBlockers: concurrencyLevel, maxAsyncWork: 0, initialCount: concurrencyLevel * 2);
                _backPressure.ZeroRef(ref _backPressure, _asyncTasks.Token);
            }
        }

        private readonly string _description; 
        private volatile int _zeroed;
        private IIoZeroSemaphore _syncRoot;
        private IIoZeroSemaphore _pressure;
        private IIoZeroSemaphore _backPressure;
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();
        private IoHeap<IoZNode> _nodeHeap;
        
        private volatile IoZNode _tail = null;
        private volatile IoZNode _head = null;
        private volatile int _count;
        private readonly bool _enableBackPressure;
        
        public int Count => _count;
        public IoZNode Tail => _tail;
        public IoZNode Head => _head;

        public IoHeap<IoZNode> NodeHeap => _nodeHeap;
        public IIoZeroSemaphore Pressure => _backPressure;

        /// <summary>
        /// Whether the collection has been modified
        /// </summary>
        private volatile bool _modified;

        public bool Modified => _modified;


        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T,TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            try
            {
                #if DEBUG
                if (zero && nanite != null && nanite is not IIoNanite)
                    throw new ArgumentException($"{_description}: {nameof(nanite)} must be of type {typeof(IIoNanite)}");
                #endif
                
                if (Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0 || !await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false))
                    return true;

                if (!_asyncTasks.IsCancellationRequested)
                    _asyncTasks.Cancel();

                if (op != null)
                {
                    var cur = Head;
                    while (cur != null)
                    {
                        _modified = true;
                        try
                        {
                            if (!zero)
                                await op(cur.Value, nanite).FastPath().ConfigureAwait(false);
                            else
                            {
                                if(!((IIoNanite)cur.Value).Zeroed())
                                    await ((IIoNanite)cur.Value).ZeroAsync((IIoNanite)nanite?? new IoNanoprobe($"{nameof(IoQueue<T>)}: {_description}")).FastPath().ConfigureAwait(false);
                            }
                        }
                        catch(Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Trace($"{_description}: {op}, {cur.Value}, {nanite}, {e.Message}");
                        }

                        cur = cur.Next;
                    }
                }

                _tail = null;
                _head = null;
                _iteratorIoZNode = null;

                await _nodeHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
                _nodeHeap = null;

                _asyncTasks.Dispose();
                _asyncTasks = null;

                _pressure?.Zero();
                _backPressure?.Zero();

                //unmanaged
                _pressure = null;
                _backPressure = null;

            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
            }

            _syncRoot.Zero();
            _syncRoot = null;

            Dispose();

            return true;
        }

        /// <summary>
        /// Blocking enqueue at the front
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item's linked list node</returns>
        public async ValueTask<IoZNode> PushAsync(T item)
        {
            var blocked = false;
            try
            {
                if (_zeroed > 0 || item == null)
                    return null;

                //wait on back pressure
                if (_enableBackPressure && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(false))
                    return null;

                var node = await _nodeHeap.TakeAsync().FastPath().ConfigureAwait(false);
                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.MaxSize}, count = {_count}");

                //set value
                node.Value = item;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                {
                    await _nodeHeap.ReturnAsync(node).FastPath().ConfigureAwait(false); ;
                    return null;
                }
                blocked = true;
                
                node.Prev = _head;
                if (_head == null)
                {
                    _tail = _head = node;
                }
                else if (_head.Prev != null)
                {
                    _head.Prev.Next = node;
                }

                _head = node;
                
                Interlocked.Increment(ref _count);

                return node;
            }
            finally
            {
                if(blocked)
                    await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);

                if(_pressure != null)
                    await _pressure.ReleaseAsync().FastPath().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Blocking enqueue item at the back
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item node</returns>
        public async ValueTask<IoZNode> EnqueueAsync(T item)
        {
            if (_zeroed > 0 || item == null)
                return null;
            
            var blocked = false;
            try
            {
                //wait on back pressure
                if (_enableBackPressure && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(false))
                        return null;

                var node = await _nodeHeap.TakeAsync().FastPath().ConfigureAwait(false);
                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.MaxSize}, count = {_count}");
                
                //set value
                node.Value = item;
                
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                {
                    await _nodeHeap.ReturnAsync(node).FastPath().ConfigureAwait(false); ;
                    return null;
                }
                blocked = true;

                //set hooks
                node.Next = _tail;

                //plumbing
                if (_tail == null)
                {
                    _tail = _head = node;
                }
                else //hook
                {
                    _tail.Prev = node;
                    _tail = node;
                }
                
                Interlocked.Increment(ref _count);
                return node;
            }
            finally
            {
                _modified = false;
                if (blocked)
                {
                    await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);    
                }
                
                if(_pressure != null)
                    await _pressure.ReleaseAsync().FastPath().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Blocking dequeue item
        /// </summary>
        /// <returns>The dequeued item</returns>
        public async ValueTask<T> DequeueAsync()
        {
            IoZNode dq = null;

            if (_zeroed > 0 || _count == 0)
            {
                return default;
            }
            
            var blocked = false;
            try
            {
                if (_pressure != null && !await _pressure.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                    return default;
                

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                    return default;

                _modified = true;
                
                blocked = true;

                //fail fast
                if (_head == null)
                {
                    return default;
                }
                
                //un-hook
                dq = _head;
                _head = _head.Prev;
                if (_head != null)
                    _head.Next = null;
                else
                    _tail = null;

                Interlocked.Decrement(ref _count);
            }
            finally
            {
                if(dq == null)
                    _modified = false;

                if (blocked)
                {
                    await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
                }
            }
            //return dequeued item
            if (dq != null)
            {
                try
                {
                    dq.Prev = null;
                    var retVal = dq.Value;
                    await _nodeHeap.ReturnAsync(dq).FastPath().ConfigureAwait(false);
                    return retVal;
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed!");
                }
                finally
                {
                    _modified = false;
                    if (_enableBackPressure)
                        await _backPressure.ReleaseAsync().FastPath().ConfigureAwait(false);
                }
                
                return default;
            }

            return default;
        }

        /// <summary>
        /// Removes a node from the queue
        /// </summary>
        /// <param name="node">The node to remove</param>
        /// <returns>True if the item was removed</returns>
        public async ValueTask<bool> RemoveAsync(IoZNode node)
        {
            if (_zeroed > 0 || node == null)
                return false;

            try
            {
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                    return false;
                _modified = true;
                //unhook
                if (node.Prev != null)
                {
                    var next = node.Next;
                    node.Prev.Next = next;

                    if (next != null)
                        next.Prev = node.Prev;
                    else //we were at the front
                        _head = node.Prev;
                }
                else //we were at the back
                {
                    _tail = _tail.Next;
                    if (_tail != null)
                        _tail.Prev = null;
                    else
                        _head = null;
                }

                Interlocked.Decrement(ref _count);
            }
            finally
            {
                _modified = false;
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
                node!.Prev = null;
                node!.Next = null;
            }

            await _nodeHeap.ReturnAsync(node).FastPath().ConfigureAwait(false);

            return true;
        }

        public bool MoveNext()
        {
            if (_count == 0)
                return false;

            _iteratorIoZNode = _iteratorIoZNode == null ? _head : _iteratorIoZNode.Prev;
            
            return _iteratorIoZNode != null;
        }

        public void Reset()
        {
            _iteratorIoZNode = null;
        }

        private volatile IoZNode _iteratorIoZNode;
        public IoZNode Current => _iteratorIoZNode;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            //TODO why does this execute?
            //_backPressure = null;
            //_asyncTasks = null;
            //_head = null;
            //_iteratorIoZNode = null;
            //_nodeHeap = null;
            //_pressure = null;
            //_syncRoot = null;
            //_tail = null;
        }

        public IEnumerator<IoZNode> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public async ValueTask ClearAsync()
        {
            try
            {
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false))
                    return;
            
                var cur = Head;
                while (cur != null)
                {
                    var tmp = cur.Prev;
                    cur.Prev = null;
                    cur.Value = default;
                    cur.Next = null;
                    await _nodeHeap.ReturnAsync(cur).FastPath().ConfigureAwait(false);
                    cur = tmp;
                }

                _count = 0;
                _head = _tail = null;
            }
            finally
            {
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
            }

        }

        /// <summary>
        /// Clip the queue
        /// </summary>
        /// <param name="cur">The new head pointer</param>
        /// <param name="count">The number of elements clipped</param>
        public void Clip(IoZNode cur, int count)
        {
            _head = cur;
            Interlocked.Add(ref _count, -count);
            if (_count == 0)
                _tail = null;
        }
    }
}
