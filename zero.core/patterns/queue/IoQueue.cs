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
    /// <typeparam name="T"></typeparam>
    public class IoZeroQueue<T>: IEnumerator<IoZeroQueue<T>.IoZNode>, IEnumerable<IoZeroQueue<T>.IoZNode>
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
        public IoZeroQueue(string description, uint capacity, int concurrencyLevel, bool enableBackPressure = false, bool disablePressure = true)
        {
            _description = description;
            _zeroSentinel = new IoNanoprobe($"{nameof(IoZeroQueue<T>)}: {description}");

            _nodeHeap = new IoHeap<IoZNode>(capacity, concurrencyLevel){Make = o => new IoZNode()};
            
            _syncRoot = new IoZeroSemaphore(description,
                maxBlockers: concurrencyLevel*2, maxAsyncWork:0, initialCount: 1);
            _syncRoot.ZeroRef(ref _syncRoot, _asyncTasks.Token);

            if (!disablePressure)
            {
                _pressure = new IoZeroSemaphore($"q pressure at {description}",
                    maxBlockers: concurrencyLevel * 2, maxAsyncWork: 0, initialCount: 0);
                _pressure.ZeroRef(ref _pressure, _asyncTasks.Token);
            }
            
            _enableBackPressure = enableBackPressure;
            if (_enableBackPressure)
            {
                _backPressure = new IoZeroSemaphore($"q back pressure at {description}",
                    maxBlockers: concurrencyLevel * 2, maxAsyncWork: 0, initialCount: 1);
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

        private volatile IoZNode _head = null;
        private volatile IoZNode _tail = null;
        private volatile int _count;
        private readonly bool _enableBackPressure;
        
        public int Count => _count;
        public IoZNode First => _head;
        public IoZNode Last => _tail;
        private readonly IoNanoprobe _zeroSentinel;

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
                    var cur = First;
                    while (cur != null)
                    {
                        try
                        {
                            if (!zero)
                                await op(cur.Value, nanite).FastPath().ConfigureAwait(false);

                            else
                            {
                                if(!((IIoNanite)cur.Value).Zeroed())
                                    await ((IIoNanite)cur.Value).ZeroAsync((IIoNanite)nanite??_zeroSentinel).FastPath().ConfigureAwait(false);
                            }
                        }
                        catch(Exception e)
                        {
                            LogManager.GetCurrentClassLogger().Trace(e,$"{_description}: {op}, {cur.Value}, {nanite}");
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

            return true;
        }

        /// <summary>
        /// Blocking enqueue item
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The enqueued item node</returns>
        public async ValueTask<IoZNode> PushAsync(T item)
        {
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

                
                node.Prev = _tail;
                if (_tail == null)
                {
                    _head = _tail = node;
                }
                else if (_tail.Prev != null)
                {
                    _tail.Prev.Next = node;
                }

                Interlocked.Increment(ref _count);

                return node;
            }
            finally
            {
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
                if(_pressure != null)
                    await _pressure.ReleaseAsync().FastPath().ConfigureAwait(false);
            }
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
                if (_zeroed > 0 || item == null)
                    return null;

                //wait on back pressure

                //var t = _backPressure.WaitAsync();
                //var r = await t.FastPath().ConfigureAwait(false);

                //if (_enableBackPressure && !r)
                //        return null;

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
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
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
            try
            {
                if (_zeroed > 0 || _count == 0)
                    return default;

                if (_pressure != null && !await _pressure.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                    return default;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
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
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
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
            try
            {
                if(_zeroed > 0 || node == null)
                    return false;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(false) || _zeroed > 0)
                    return false;

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
                await _syncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
                node!.Prev = null;
                node!.Next = null;
            }

            await _nodeHeap.ReturnAsync(node).FastPath().ConfigureAwait(false);

            return true;
        }

        public bool MoveNext()
        {
            Current = Current?.Next;
            return Current != null;
        }

        public void Reset()
        {
            Current = _head;
        }

        public IoZNode Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            
        }

        public IEnumerator<IoZNode> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
