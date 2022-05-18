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
using zero.core.patterns.queue.enumerator;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// ZeroAsync Queue
    /// </summary>
    /// <typeparam name="T">The type of item queued</typeparam>
    public class IoQueue<T>:  IEnumerable<IoQueue<T>.IoZNode>
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
        /// Q modes
        /// </summary>
        [Flags]
        public enum Mode
        {
            Undefined = 0,
            Pressure = 1,
            BackPressure = 1<<1,
            DynamicSize = 1 << 2,
        }
        /// <summary>
        /// constructor
        /// </summary>
        public IoQueue(string description, int capacity, int concurrencyLevel, Mode configuration = Mode.Undefined)
        {
#if DEBUG
            _description = description;
            string desc = $"{nameof(_nodeHeap)}: {_description}";
#else
            var desc = _description = string.Empty;
#endif
            _configuration = configuration;
            _nodeHeap = new IoHeap<IoZNode>(desc, capacity, static (_,_) => new IoZNode(), _configuration.HasFlag(Mode.DynamicSize)) {
                PopAction =
                (node, _) =>
                {
                    node.Next = null;
                    node.Prev = null;
                }
            };

            //_syncRoot = new IoZeroSemaphore<bool>(desc, maxBlockers: concurrencyLevel, initialCount: 1, cancellationTokenSource: _asyncTasks, runContinuationsAsynchronously: true);
            //_syncRoot.ZeroRef(ref _syncRoot, _ => true);
            _syncRoot = new IoZeroCore<bool>(desc, concurrencyLevel, 1);
            _syncRoot.ZeroRef(ref _syncRoot, _ => true);

            if (_configuration.HasFlag(Mode.Pressure))
            {
                _pressure = new IoZeroCore<bool>($"qp {description}", concurrencyLevel);
                _pressure.ZeroRef(ref _pressure, _ => true);
            }
            
            if (_configuration.HasFlag(Mode.BackPressure))
            {
                _backPressure = new IoZeroCore<bool>($"qbp {description}",concurrencyLevel, concurrencyLevel, false);
                _backPressure.ZeroRef(ref _backPressure, _ => true);
            }

            _curEnumerator = new IoQueueEnumerator<T>(this);
        }

        #region memory
        private readonly string _description; 
        private volatile int _zeroed;
        private readonly IIoZeroSemaphoreBase<bool> _syncRoot;
        private readonly IIoZeroSemaphoreBase<bool> _pressure;
        private readonly IIoZeroSemaphoreBase<bool> _backPressure;
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();
        private IoHeap<IoZNode> _nodeHeap;
        
        private volatile IoZNode _head;
        private volatile IoZNode _tail;
        private volatile int _count;
        #endregion

        public int Capacity => _nodeHeap.Capacity;
        public int Count => _count;
        public IoZNode Head => _head;
        public IoZNode Tail => _tail;

        public IoHeap<IoZNode> NodeHeap => _nodeHeap;
        public IIoZeroSemaphoreBase<bool> Pressure => _backPressure;

        private IoQueueEnumerator<T> _curEnumerator;

        public bool Modified
        {
            get => _curEnumerator.Modified;
            set => _curEnumerator.Modified = value;
        }

        public bool Zeroed => _zeroed > 0 || _pressure != null && _pressure.Zeroed() || _backPressure != null && _backPressure.Zeroed();

        public bool IsAutoScaling => _nodeHeap.IsAutoScaling;
        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<IoZNode, TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            if (zero && (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0))
                return true;

            //Prime zero
            if (zero && !_asyncTasks.IsCancellationRequested)
                _asyncTasks.Cancel();

            try
            {
                if (!await _syncRoot.WaitAsync().FastPath())
                {
                    return false;
                }
                Debug.Assert(_syncRoot.Zeroed() || _syncRoot.ReadyCount == 0);

#if DEBUG
                if (zero && nanite != null && nanite is not IIoNanite)
                    throw new ArgumentException(
                        $"{_description}: {nameof(nanite)} must be of type {typeof(IIoNanite)}");
#endif

                var cur = _head;
                while (cur != null)
                {
                    try
                    {
                        if (op != null)
                            await op(cur, nanite).FastPath();
                        if (cur.Value is IIoNanite ioNanite)
                        {
                            if (!ioNanite.Zeroed())
                                await ioNanite.DisposeAsync(nanite as IIoNanite, string.Empty).FastPath();
                        }
                    }
                    catch when(Zeroed){}
                    catch (Exception e)when(!Zeroed)
                    {
                        LogManager.GetCurrentClassLogger()
                            .Trace($"{_description}: {op}, {cur.Value}, {nanite}, {e.Message}");
                    }

                    cur = cur.Next;
                }
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                _syncRoot.EnsureRelease(true);
                
                if (zero)
                {
                    await ClearAsync().FastPath(); //TODO perf: can these two steps be combined?
                    await _nodeHeap.ZeroManagedAsync<object>().FastPath();

                    _nodeHeap = null;
                    _count = 0;
                    _head = null;
                    _tail = null;

                    _asyncTasks.Cancel();
                    _asyncTasks.Dispose();
                    _asyncTasks = null;

                    _pressure?.ZeroSem();
                    _backPressure?.ZeroSem();

                    //unmanaged
                    _syncRoot.ZeroSem();
                }
            }

            return true;
        }

        /// <summary>
        /// Blocking enqueue at the back
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item's linked list node</returns>
        public ValueTask<IoZNode> EnqueueAsync(T item)
        {
            return EnqueueAsync<object>(item);
        }

        /// <summary>
        /// Blocking enqueue at the back
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <param name="onAtomicAdd">Additional actions to perform in the critical area</param>
        /// <param name="context">atomic context</param>
        /// <returns>The queued item's linked list node</returns>
        public async ValueTask<IoZNode> EnqueueAsync<TC>(T item, Func<TC,ValueTask> onAtomicAdd = null, TC context = default)
        {
            var entered = false;
            IoZNode retVal = default;

            if (_zeroed > 0 || item == null)
                return null;

            try
            {
                //wait on back pressure
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath() || _zeroed > 0)
                {
                    if(_zeroed == 0 && (!_backPressure?.Zeroed()??true))
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(EnqueueAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                    return null;
                }
                
                var node = _nodeHeap.Take();
                if(node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.Capacity}, count = {_count}, \n {Environment.StackTrace}");

                node.Value = item;
                
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (!await _syncRoot.WaitAsync().FastPath())
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

                Interlocked.Increment(ref _count);
                return retVal = node;
            }
            finally
            {
                var success = _tail != null && _tail == retVal;

                try
                {
                    if (entered)
                    {
                        //additional atomic actions
                        try
                        {
                            if (onAtomicAdd != null)
                                await onAtomicAdd.Invoke(context).FastPath();
                        }
                        catch when (Zeroed)
                        {
                        }
                        catch (Exception e) when (!Zeroed)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, $"{nameof(EnqueueAsync)}");
                        }
                        finally
                        {
                            _syncRoot.EnsureRelease(true);
                        }
                    }

                    if (_pressure != null && success)
                        _pressure.EnsureRelease(true, true);
                    else
                        _backPressure?.EnsureRelease(true);
                }
                catch when (Zeroed) { }
                catch (Exception e) when (!Zeroed)
                {
                    LogManager.GetCurrentClassLogger().Error(e, $"{nameof(EnqueueAsync)}");
                }
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
                return null;
            }
            
            var entered = false;
            IoZNode retVal = default;
            try
            {
                //wait on back pressure
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath() || _zeroed > 0)
                {
                    if (_zeroed == 0 && (!_backPressure?.Zeroed()??true))
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(PushBackAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                    return null;
                }

                var node = _nodeHeap.Take();
                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.Capacity}, count = {_count}");
                
                node.Value = item;

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (!await _syncRoot.WaitAsync().FastPath())
                {
                    _nodeHeap.Return(node);
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(PushBackAsync)}: _syncRoot failure ~> {_syncRoot}");
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

                Interlocked.Increment(ref _count);
                return retVal = node;
            }
            finally
            {
                if (entered)
                    _syncRoot.EnsureRelease(true);

                if (retVal != default)
                    _pressure?.EnsureRelease(true);
                else
                    _backPressure?.EnsureRelease(true);
            }
        }

        private volatile int _entered = 0;
        private readonly Mode _configuration;

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

            var entered = false;
            try
            {
                if (_pressure != null && !await _pressure.WaitAsync().FastPath())
                    return default;

                if (!await _syncRoot.WaitAsync().FastPath())
                    return default;

                Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed());
                Debug.Assert(Interlocked.Increment(ref _entered) == 1);
                entered = true;
                Debug.Assert(_entered < 2);

                if (_count == 0)
                    return default;

                dq = _head;
                _head = _head.Next;
                dq.Next = null;
                dq.Prev = null;
                if (_head != null)
                    _head.Prev = null;
                else
                    _tail = null;

                _curEnumerator.Modified = true;
                Interlocked.Decrement(ref _count);
            }
            catch when (_zeroed > 0) { }
            catch (Exception e) when (_zeroed == 0)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed! {nameof(_count)} = {_count}, {nameof(_tail)} = {_tail}, {nameof(_head)} = {_head}, heap => {_nodeHeap.Description}");
            }
            finally
            {
                if (entered)
                {
                    Interlocked.Decrement(ref _entered);
                    Debug.Assert(_syncRoot.ReadyCount == 0);
                    _syncRoot.Release(true);
                    
                }

                //var t = _syncRoot.ReadyCount == 1 || _entered > 0;
                //Debug.Assert(t);
            }
            
            try
            {
                if (dq != null)
                {
                    var retVal = dq.Value;
                    dq.Value = default;
                    _nodeHeap.Return(dq);
                    _backPressure?.EnsureRelease(true, true);//TODO: Why does this one need to be false? Every time it is set to async strange things start to happen.
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
            Debug.Assert(_backPressure == null);
            Debug.Assert(node != null);

            if (_count == 0)
                return false;

            var deDup = true;
            try
            {
                if (!await _syncRoot.WaitAsync().FastPath())
                    return false;

                if (node.Value == null)
                    return true;
                
                deDup = false;

                if (node.Prev != null)
                {
                    var next = node.Next;
                    node.Prev.Next = next;

                    if (next != null)
                        next.Prev = node.Prev;
                    else
                    {
                        _tail = node.Prev;
                        _tail.Next = null;
                    }
                }
                else
                {
                    if( _head != node)
                    {
                        node.Prev = null;
                        return false;
                    }
                        
                    _head = _head.Next;
                    node.Next = null;
                    if (_head != null)
                        _head.Prev = null;
                    else
                        _tail = null;
                }

                _curEnumerator.Modified = true;
                Interlocked.Decrement(ref _count);
                return true;
            }
            finally
            {
                _syncRoot.EnsureRelease(true);
                node.Value = default;
                _nodeHeap.Return(node, deDup);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<IoZNode> GetEnumerator()
        {
            //_curEnumerator = (IoQueueEnumerator<T>)_curEnumerator.Reuse(this, c => new IoQueueEnumerator<T>((IoQueue<T>)c));
            //_curEnumerator.Reset();
            //return _curEnumerator;
            return _curEnumerator = new IoQueueEnumerator<T>(this);
        }

        /// <summary>
        /// Get a fresh enumerator
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Clear the Queue
        /// </summary>
        /// <returns>A valuetask</returns>
        public async ValueTask ClearAsync()
        {
            try
            {
                if (!await _syncRoot.WaitAsync().FastPath())
                    return;

                var insane = _count * 2;
                var cur = _head;
                while (cur != null && insane --> 0)
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
                _syncRoot.EnsureRelease(true);
            }
        }

        /// <summary>
        /// Clip the queue
        /// </summary>
        /// <param name="crisper">The new head pointer</param>
        public async ValueTask ClipAsync(IoZNode crisper)
        {
            try
            {
                if (!await _syncRoot.WaitAsync().FastPath())
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
                _syncRoot.EnsureRelease(true);
            }
        }

        /// <summary>
        /// Reset current enumerator
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            if (_curEnumerator != null && _curEnumerator.Disposed == 0)
            {
                _curEnumerator.Reset();
                _curEnumerator.Modified = false;
            }
            else
            {
                _ = GetEnumerator();
            }
        }

    }
}
