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
        /// constructor
        /// </summary>
        public IoQueue(string description, int capacity, int concurrencyLevel, int prefetch = 1, bool enableBackPressure = false, bool disablePressure = true, bool autoScale = false)
        {
#if DEBUG
            _description = description;
            string desc = $"{nameof(_nodeHeap)}: {_description}";
#else
            var desc = _description = string.Empty;
#endif

            _nodeHeap = new IoHeap<IoZNode>(desc, capacity, autoScale: autoScale) {Malloc = static (_,_) => new IoZNode(), PopAction =
                (node, _) =>
                {
                    node.Next = null;
                    node.Prev = null;
                }
            };

            _syncRoot = new IoZeroSemaphore(desc, maxBlockers: concurrencyLevel, initialCount: 1, asyncWorkerCount: 0);
            _syncRoot.ZeroRef(ref _syncRoot, _asyncTasks);

            var c = Math.Max(prefetch + 1, concurrencyLevel);

            if (!disablePressure)
            {
                _pressure = new IoZeroSemaphore($"qp {description}",
                    maxBlockers: c, asyncWorkerCount: 0);
                _pressure.ZeroRef(ref _pressure, _asyncTasks);
            }
            
            if (enableBackPressure)
            {
                _backPressure = new IoZeroSemaphore($"qbp {description}",
                    maxBlockers: c, asyncWorkerCount: 0, initialCount: concurrencyLevel);
                _backPressure.ZeroRef(ref _backPressure, _asyncTasks);
            }

            _curEnumerator = new IoQueueEnumerator<T>(this);
        }

        #region memory
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
        #endregion

        public int Capacity => _nodeHeap.Capacity;
        public int Count => _count;
        public IoZNode Head => _head;
        public IoZNode Tail => _tail;

        public IoHeap<IoZNode> NodeHeap => _nodeHeap;
        public IIoZeroSemaphore Pressure => _backPressure;

        private IoQueueEnumerator<T> _curEnumerator;

        public bool Modified
        {
            get => _curEnumerator.Modified;
            set => _curEnumerator.Modified = value;
        }

        public bool Zeroed => _zeroed > 0 || _pressure != null && _pressure.Zeroed() || _backPressure != null && _backPressure.Zeroed();

        public bool IsAutoScaling => _nodeHeap.IsAutoScaling;
        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T,TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return true;

            //Prime zero
            if (zero && !_asyncTasks.IsCancellationRequested)
                _asyncTasks.Cancel();

            try
            {
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc))
                {
                    return false;
                }

#if DEBUG
                if (zero && nanite != null && nanite is not IIoNanite)
                    throw new ArgumentException(
                        $"{_description}: {nameof(nanite)} must be of type {typeof(IIoNanite)}");
#endif

                var cur = _head;
                while (cur != null)
                {
                    _curEnumerator.Modified = true;
                    try
                    {
                        if (op != null)
                            await op(cur.Value, nanite).FastPath().ConfigureAwait(Zc);
                        if (cur.Value is IIoNanite ioNanite)
                        {
                            if (!ioNanite.Zeroed())
                                await ioNanite.Zero(nanite as IIoNanite, string.Empty).FastPath().ConfigureAwait(Zc);
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
                _syncRoot.Release();

                if (zero)
                {
                    await ClearAsync().FastPath().ConfigureAwait(Zc); //TODO perf: can these two steps be combined?
                    await _nodeHeap.ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);

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
                    _pressure = null;
                    _backPressure = null;

                    _syncRoot.ZeroSem();
                    _syncRoot = null;
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
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
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
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || !(entered = true) || _zeroed > 0)
                {
                    _nodeHeap.Return(node);
                    return null;
                }

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
                                await onAtomicAdd.Invoke(context).FastPath().ConfigureAwait(Zc);
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
                            _syncRoot.Release();
                        }
                    }

                    if (_pressure!= null && success)
                        _pressure.Release();
                    else
                        _backPressure?.Release();
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
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath().ConfigureAwait(Zc) || _zeroed > 0)
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
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || !(entered = true) || _zeroed > 0)
                {
                    _nodeHeap.Return(node);
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(DequeueAsync)}: _syncRoot failure ~> {_syncRoot}");
                    return null;
                }

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
                var success = retVal != default;

                if (entered)
                    _syncRoot.Release();

                if(success)
                    _pressure?.Release();
                else
                    _backPressure?.Release();
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

                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc) || !(blocked = true) || _zeroed > 0)
                    return default;

                if (_count == 0)
                    return default;

                _curEnumerator.Modified = true;

                dq = _head;
                _head = _head.Next;
                dq.Next = null;
                dq.Prev = null;
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
                if (blocked)
                    _syncRoot.Release();
            }
            
            try
            {
                if (dq != null)
                {
                    var retVal = dq.Value;
                    dq.Value = default;
                    _nodeHeap.Return(dq);
                    _backPressure?.Release();
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
                if (!await _syncRoot.WaitAsync().FastPath().ConfigureAwait(Zc))
                {
                    return false;
                }
                
                deDup = false;
                _curEnumerator.Modified = true;

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
                    Debug.Assert(_head == node);
                    _head = _head.Next;
                    node.Next = null;
                    if (_head != null)
                        _head.Prev = null;
                    else
                        _tail = null;
                }

                Interlocked.Decrement(ref _count);
                return true;
            }
            finally
            {
                _syncRoot.Release();
                node.Value = default;
                _nodeHeap.Return(node, deDup);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<IoZNode> GetEnumerator()
        {
            _curEnumerator = (IoQueueEnumerator<T>)_curEnumerator.Reuse(this, c => new IoQueueEnumerator<T>((IoQueue<T>)c));
            _curEnumerator.Reset();
            return _curEnumerator;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
                _syncRoot.Release();
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            _curEnumerator = new IoQueueEnumerator<T>(this);
            _curEnumerator.Modified = false;
        }

    }
}
