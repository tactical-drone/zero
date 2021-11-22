using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.queue
{

    /// <summary>
    /// ZeroAsync Queue
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
        public IoQueue(string description, int capacity, int concurrencyLevel, bool enableBackPressure = false, bool disablePressure = true)
        {
            _description = description;

            _nodeHeap = new IoHeap<IoZNode>($"{nameof(_nodeHeap)}: {_description}", capacity){Make = static (o,s) => new IoZNode()};
            
            _syncRoot = new IoZeroSemaphore($"{nameof(_syncRoot)} {description}",
                maxBlockers: concurrencyLevel, initialCount: 1);
            _syncRoot.ZeroRef(ref _syncRoot, _asyncTasks);

            //_syncRoot = new IoZeroRefMut(_asyncTasks.Token);

            if (!disablePressure)
            {
                _pressure = new IoZeroSemaphore($"qp {description}",
                    maxBlockers: concurrencyLevel, concurrencyLevel: 0, initialCount: 0);
                _pressure.ZeroRef(ref _pressure, _asyncTasks);
            }
            
            if (enableBackPressure)
            {
                _backPressure = new IoZeroSemaphore($"qbp {description}",
                    maxBlockers: concurrencyLevel, concurrencyLevel: 0, initialCount: concurrencyLevel);
                _backPressure.ZeroRef(ref _backPressure, _asyncTasks);
            }

            Reset();
        }

        private readonly string _description; 
        private volatile int _zeroed;
        private readonly bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private IIoZeroSemaphore _syncRoot;
        private IIoZeroSemaphore _pressure;
        private IIoZeroSemaphore _backPressure;
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();
        private IoHeap<IoZNode> _nodeHeap;
        
        private volatile IoZNode _head;
        private volatile IoZNode _tail;
        private volatile int _count;

        public int Count => _count;
        public IoZNode Head => _head;
        public IoZNode Tail => _tail;

        public IoHeap<IoZNode> NodeHeap => _nodeHeap;
        public IIoZeroSemaphore Pressure => _backPressure;

        /// <summary>
        /// Whether the collection has been modified
        /// </summary>
        private volatile bool _modified;

        public bool Modified => _modified;

        public bool Zeroed => _zeroed > 0 || _syncRoot.Zeroed() || _pressure != null && _pressure.Zeroed() || _backPressure != null && _backPressure.Zeroed();

        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T,TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            try
            {
#if DEBUG
                if (zero && nanite != null && nanite is not IIoNanite)
                    throw new ArgumentException(
                        $"{_description}: {nameof(nanite)} must be of type {typeof(IIoNanite)}");
#endif

                if (Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
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
                                await op(cur.Value, nanite).FastPath().ConfigureAwait(Zc);
                            else
                            {
                                if (!((IIoNanite)cur.Value).Zeroed())
                                    ((IIoNanite)cur.Value).Zero((IIoNanite)nanite ??
                                                                new IoNanoprobe(
                                                                    $"{nameof(IoQueue<T>)}: {_description}"));
                            }
                        }
                        catch (Exception e)
                        {
                            LogManager.GetCurrentClassLogger()
                                .Trace($"{_description}: {op}, {cur.Value}, {nanite}, {e.Message}");
                        }

                        cur = cur.Next;
                    }
                }

            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                if (zero)
                {
                    _head = null;
                    _tail = null;
                    _iteratorIoZNode = null;

                    await _nodeHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);
                    _nodeHeap = null;

                    _asyncTasks.Dispose();
                    _asyncTasks = null;

                    _pressure?.ZeroSem();
                    _backPressure?.ZeroSem();

                    //unmanaged
                    _pressure = null;
                    _backPressure = null;

                    _syncRoot.ZeroSem();
                    _syncRoot = null;

                    Dispose();

                }
            }

            return true;
        }

        /// <summary>
        /// Blocking enqueue at the back
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item's linked list node</returns>
        public async ValueTask<IoZNode> EnqueueAsync(T item)
        {
            var entered = false;
            IoZNode retVal = default;
            try
            {
                if (_zeroed > 0 || item == null)
                    return null;

                //wait on back pressure
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(Zc) ||
                    _zeroed > 0)
                {
                    if(_zeroed == 0 && !_backPressure.Zeroed())
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(EnqueueAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                    return null;
                }
                
                var node = _nodeHeap.Take();
                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.MaxSize}, count = {_count}");

                node.Value = item;
                node.Prev = null;
                node.Next = null;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
                {
                    _nodeHeap.Return(node);
                    return null;
                }
                entered = true;
                
                if (_tail == null)
                {
                    _head = _tail = node;
                }
                else 
                {
                    node.Prev = _tail;
                    _tail.Next = node;
                    _tail = node;
                }

                return retVal = node;
            }
            finally
            {
                var success = _tail == retVal;

                if (success)
                    Interlocked.Increment(ref _count);

                if (entered)
                    _syncRoot.Release();

                if (success)
                    _pressure?.Release();
            }
        }

        /// <summary>
        /// Blocking enqueue item at the head of the Q
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item node</returns>
        public async ValueTask<IoZNode> PushBackAsync(T item)
        {
            if (_zeroed > 0 || item == null)
            {
                Debug.Assert(item != null);
                return null;
            }
            
            var entered = false;
            IoZNode retVal = default;
            try
            {
                //wait on back pressure
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
                {
                    if (_zeroed == 0 && !_backPressure.Zeroed())
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(PushBackAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                    return null;
                }

                var node = _nodeHeap.Take();
                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.MaxSize}, count = {_count}");
                
                node.Value = item;
                node.Prev = null;
                node.Next = null;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
                {
                    _nodeHeap.Return(node);
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(DequeueAsync)}: _syncRoot failure ~> {_syncRoot}");
                    return null;
                }
                entered = true;

                if (_head == null)
                {
                    _head = _tail = node;
                }
                else
                {
                    node.Next = _head;
                    _head.Prev = node;
                    _head = node;
                }
                
                return retVal = node;
            }
            finally
            {
                var success = retVal != null;

                if (success)
                    Interlocked.Increment(ref _count);

                if (entered)
                    _syncRoot.Release();

                if(success)
                    _pressure?.Release();
            }
        }

        /// <summary>
        /// Blocking dequeue item
        /// </summary>
        /// <returns>The dequeued item</returns>
        public async ValueTask<T> DequeueAsync()
        {
            IoZNode dq = null;

            if (_zeroed > 0 || _count == 0 && _pressure == null)
            {
                return default;
            }

            var blocked = false;
            try
            {
                if (_pressure != null && !await _pressure.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
                    return default;

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
                    return default;

                blocked = true;

                if (_count == 0)
                    return default;

                _modified = true;

                dq = _head;
                _head = _head.Next;

                if (_head != null)
                    _head.Prev = null;
                else
                    _tail = null;

                Interlocked.Decrement(ref _count);
            }
            catch when (_zeroed > 0) { }
            catch (Exception e) when (_zeroed == 0)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed! {nameof(_count)} = {_count}, {nameof(_tail)} = {_tail}, {nameof(_head)} = {_head}, heap => {_nodeHeap.Description}");
            }
            finally
            {
                if (_backPressure != null && _backPressure.Release() == -1)
                {
                    if(_zeroed == 0 && !_backPressure.Zeroed())
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(DequeueAsync)}.{nameof(_backPressure.Release)}: back pressure release failure ~> {_backPressure}");
                }

                if (blocked)
                    _syncRoot.Release();
            }
            //return dequeued item
            
            try
            {
                if (dq != null)
                {
                    var retVal = dq.Value;
                    _nodeHeap.Return(dq);
                    return retVal;
                }
            }
            catch when(_zeroed > 0){}
            catch (Exception e)when(_zeroed == 0)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed!");
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
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
                    return false;
                _modified = true;

                if (node.Prev != null)
                {
                    var next = node.Next;
                    node.Prev.Next = next;

                    if (next != null)
                        next.Prev = node.Prev;
                    else 
                        _tail = node.Prev;
                }
                else
                {
                    _head = _head.Next;
                    if (_head != null)
                        _head.Prev = null;
                    else
                        _tail = null;
                }

                Interlocked.Decrement(ref _count);
            }
            finally
            {
                _syncRoot.Release();
            }

            _nodeHeap.Return(node);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            if (_count == 0 || _iteratorIoZNode == null)
                return false;

            _iteratorIoZNode = _iteratorIoZNode.Next;
            
            return _iteratorIoZNode != null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Reset()
        {
            _modified = false;
            _iteratorIoZNode = new IoZNode {Next = _head};
        }

        private volatile IoZNode _iteratorIoZNode;
        public IoZNode Current => _iteratorIoZNode;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            //TODO why does this execute?
            //_backPressure = null;
            //_asyncTasks = null;
            //_tail = null;
            //_iteratorIoZNode = null;
            //_nodeHeap = null;
            //_pressure = null;
            //_syncRoot = null;
            //_head = null;
        }

        public IEnumerator<IoZNode> GetEnumerator()
        {
            Reset();
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
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc))
                    return;
            
                var cur = Head;
                while (cur != null)
                {
                    var tmp = cur.Next;
                    cur.Prev = null;
                    cur.Value = default;
                    cur.Next = null;
                    _nodeHeap.Return(cur);
                    cur = tmp;
                }

                _count = 0;
                _tail = _head = null;
            }
            finally
            {
                _syncRoot.Release();
            }
        }

        /// <summary>
        /// Clip the queue
        /// </summary>
        /// <param name="crisper">The new head pointer</param>
        /// <param name="count">The number of elements clipped</param>
        public async ValueTask ClipAsync(IoZNode crisper)
        {
            try
            {
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc))
                    return;

                var cur = _head;
                var c = 0;
                while (cur != crisper)
                {
                    _nodeHeap.Return(cur);
                    cur = cur.Next;
                    c++;
                }

                _head = crisper;
                Interlocked.Add(ref _count, -c);
                if (_count == 0)
                    _tail = null;
            }
            finally
            {
                _syncRoot.Release();
            }
        }
    }
}
