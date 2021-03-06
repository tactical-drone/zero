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
            public volatile IoZNode Next;
            public volatile IoZNode Prev;
            public T Value;
            public int Qid;
            //public int lastOp;
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

            capacity = configuration.HasFlag(Mode.DynamicSize) ? short.MaxValue : capacity * 2;

            _nodeHeap = new IoHeap<IoZNode>(desc, capacity, static (_,_) => new IoZNode(), configuration.HasFlag(Mode.DynamicSize)) {
                PushAction = 
                node =>
                {
                    Interlocked.Increment(ref node.Qid);
                    node.Next = null;
                    node.Prev = null;
                    node.Value = default;
                },
                PopAction = (node, _) =>
                {
                    //node.lastOp = -1;
                    Interlocked.Increment(ref node.Qid);
                }
            };

            //_syncRoot = new IoZeroSemaphore<bool>(desc, maxBlockers: concurrencyLevel, initialCount: 1, cancellationTokenSource: _asyncTasks, runContinuationsAsynchronously: true);
            //_syncRoot.ZeroRef(ref _syncRoot, _ => true);

            IIoZeroSemaphoreBase<int> q = new IoZeroCore<int>(desc, concurrencyLevel, _asyncTasks,1, false);
            _syncRoot = q.ZeroRef(ref q, _ => Environment.TickCount);

            if (configuration.HasFlag(Mode.Pressure))
            {
                _pressure = new IoZeroCore<bool>($"qp {description}", concurrencyLevel, _asyncTasks, zeroAsyncMode:true);
                _pressure.ZeroRef(ref _pressure, _ => true);
            }
            
            if (configuration.HasFlag(Mode.BackPressure))
            {
                _backPressure = new IoZeroCore<bool>($"qbp {description}",concurrencyLevel, _asyncTasks,concurrencyLevel, false);
                _backPressure.ZeroRef(ref _backPressure, _ => true);
            }

            _curEnumerator = new IoQueueEnumerator<T>(this);
        }

        #region memory
        private readonly string _description; 
        private volatile int _zeroed;
        private readonly IIoZeroSemaphoreBase<int> _syncRoot;
        private readonly IIoZeroSemaphoreBase<bool> _pressure;
        private readonly IIoZeroSemaphoreBase<bool> _backPressure;
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();
        private IoHeap<IoZNode> _nodeHeap;
        
        private volatile IoZNode _head;
        private volatile IoZNode _tail;
        private volatile int _count;
        private long _operations;
        #endregion

        public string Description => $"{_description}, ops = {_operations}, count = {_count}, head = {_head}, tail = {_tail}, open = {_syncRoot.ReadyCount}, blocking = {_syncRoot.WaitCount}, z = {Zeroed}";
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
                await _syncRoot.WaitAsync().FastPath();
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
                else
                {
                    _syncRoot.Release(Environment.TickCount);
                }
            }

            return true;
        }

        /// <summary>
        /// Blocking enqueue at the back
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item's linked list node</returns>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
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

                await _syncRoot.WaitAsync().FastPath();
                entered = true;
#if DEBUG
                //Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed(), $"{nameof(_insaneExclusive)} = {_insaneExclusive} > 1");
                //Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed(), $"{nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount} [INVALID], wait = {_syncRoot.WaitCount}");
                //Debug.Assert(_insaneExclusive < 2, $"{nameof(_insaneExclusive)} = {_insaneExclusive} > 1");
                Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed());
                Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed());
                Debug.Assert(_insaneExclusive < 2);
#endif
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
                Interlocked.Increment(ref _operations);
                return node;
            }
            finally
            {
                if (!Zeroed)
                {
                    try
                    {
                        if (_pressure != null && _tail != null)
                            _pressure.Release(true, true);
                        else
                            _backPressure?.Release(true, true);

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
#if DEBUG
                                Interlocked.Decrement(ref _insaneExclusive);
#endif
                                _syncRoot.Release(Environment.TickCount, false);//FALSE
                            }
                        }
                    }
                    catch when (Zeroed) { }
                    catch (Exception e) when (!Zeroed)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, $"{nameof(EnqueueAsync)}");
                    }
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
                if (_backPressure != null && !await _backPressure.WaitAsync().FastPath())
                {
                    if (_zeroed == 0 && (!_backPressure?.Zeroed()??true))
                        LogManager.GetCurrentClassLogger().Fatal($"{nameof(PushBackAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                    return null;
                }

                var node = _nodeHeap.Take();

                if (node == null)
                    throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.Capacity}, count = {_count}");
                
                node.Value = item;

                await _syncRoot.WaitAsync().FastPath();
                entered = true;
#if DEBUG
                //Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed(), $"{nameof(_insaneExclusive)} = {_insaneExclusive} > 1");
                //Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed(), $"{nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount} [INVALID]");
                //Debug.Assert(_insaneExclusive < 2 || _syncRoot.Zeroed());
                Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed());
                Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed());
                Debug.Assert(_insaneExclusive < 2 || _syncRoot.Zeroed());
#endif
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
                Interlocked.Increment(ref _operations);
                return retVal = node;
            }
            finally
            {
                if (!Zeroed)
                {
                    if (retVal != default)
                        _pressure?.Release(true, true);
                    else
                        _backPressure?.Release(true, true);

                    if (entered)
                    {
#if DEBUG
                        Interlocked.Decrement(ref _insaneExclusive);
#endif
                        _syncRoot.Release(Environment.TickCount, false);
                    }
                }
            }
        }

#if DEBUG
        private volatile int _insaneExclusive = 0;
#endif
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
            T retVal = default;
            try
            {
                if (_pressure != null && !await _pressure.WaitAsync().FastPath())
                    return default;

                await _syncRoot.WaitAsync().FastPath();
                entered = true;
#if DEBUG
                //Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed(), $"{nameof(_insaneExclusive)} = {_insaneExclusive} > 1, {nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount}, wait =  {_syncRoot.WaitCount}");
                //Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed(), $"{nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount} [INVALID], wait =  {_syncRoot.WaitCount}");
                //Debug.Assert(_insaneExclusive < 2 || _syncRoot.Zeroed() || Zeroed, $"{nameof(_insaneExclusive)}: {_insaneExclusive}");
                Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed());
                Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed());
                Debug.Assert(_insaneExclusive < 2);
#endif
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
                Interlocked.Increment(ref _operations);
            }
            catch when (_zeroed > 0) { }
            catch (Exception e) when (_zeroed == 0)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed! {nameof(_count)} = {_count}, {nameof(_tail)} = {_tail}, {nameof(_head)} = {_head}, heap => {_nodeHeap.Description}");
            }
            finally
            {
                try
                {
                    if (dq != null)
                    {
                        retVal = dq.Value;
                        //dq.lastOp = 1;
                        _nodeHeap.Return(dq);
                        _backPressure?.Release(true, false);//FALSE
                    }
                }
                catch when (_zeroed > 0) { }
                catch (Exception e) when (_zeroed == 0)
                {
                    LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed!");
                }

                if (entered && !Zeroed)
                {
#if DEBUG
                    Interlocked.Decrement(ref _insaneExclusive);
                    //Debug.Assert(_syncRoot.ReadyCount == 0, $"INVALID {nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount}");
                    Debug.Assert(_syncRoot.ReadyCount == 0);
#endif
                    _syncRoot.Release(Environment.TickCount, false);//FALSE!
                }
            }
            
            return retVal;
        }

        /// <summary>
        /// [Experimental] Removes a node from inside the queue. This function is the purpose of rolling our own Q.
        ///
        /// Since nodes are cached in the heap, reentrancy support is needed for each use case iif <see cref="RemoveAsync"/> is used at all in combination with <see cref="DequeueAsync"/>
        /// Ideally, the id needs to be locked in long before this branch executes. This is not
        /// a silver bullet reentrancy support. Every use case needs to be carefully inspected.
        ///
        /// Bug can still happen if not implemented correctly!
        /// </summary>
        /// <param name="node">The node to remove</param>
        /// <param name="qId">Id used for sane reentrancy </param>
        /// <returns>True if the item was removed</returns>
        public async ValueTask<bool> RemoveAsync(IoZNode node, int qId)
        {
            Debug.Assert(_backPressure == null);
            Debug.Assert(node != null);

            if (_count == 0)
                return false;

            var deDup = true;
            try
            {
                await _syncRoot.WaitAsync().FastPath();
#if DEBUG
                //Debug.Assert(_insaneExclusive == 0 || _syncRoot.Zeroed(), $"{nameof(_insaneExclusive)} = {_insaneExclusive} > 0");
                //Interlocked.Increment(ref _insaneExclusive);
                //Debug.Assert(_insaneExclusive < 2 || _syncRoot.Zeroed());
                //Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed(), $"{nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount} [INVALID], wait = {_syncRoot.WaitCount}, exclusive ?= {_insaneExclusive}");
                Debug.Assert(_insaneExclusive == 0 || _syncRoot.Zeroed());
                Interlocked.Increment(ref _insaneExclusive);
                Debug.Assert(_insaneExclusive < 2 || _syncRoot.Zeroed());
                Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed());
#endif
                if (qId != node.Qid || node.Value == null) //TODO: BUG, something is broken here *
                {
                    LogManager.GetCurrentClassLogger().Trace($"qid = {node.Qid}, wanted = {qId}, node.Value = {node.Value}, n = {node.Next}, p = {node.Prev}");
                    return true; //true because another Remove or Dequeue has successfully raced
                }
                
                deDup = false;

                if (node.Prev != null)
                {
                    node.Prev.Next = node.Next;
                    if (node.Next != null)
                        node.Next.Prev = node.Prev;
                    else
                    {
                        Debug.Assert(node == _tail);
                        _tail = node.Prev;

                        if(_tail != null)
                            _tail.Next = null;
                        else
                            _tail = null;
                    }
                }
                else
                {
                    Debug.Assert(_head == node || Zeroed);//TODO: BUG, something is broken here *
                    //while (!(_head == node || Zeroed))
                    //{
                    //    Interlocked.MemoryBarrierProcessWide();
                    //    LogManager.GetCurrentClassLogger().Error($"{Zeroed}-> qid = {node.Qid}, wanted = {qId}, last = {node.lastOp}, _insaneExclusive = {_insaneExclusive} node.Value = {node.Value}, n = {node.Next}, p = {node.Prev}, {Description}");
                    //    //Debug.Assert(false);
                    //}

                    _head = _head.Next;
                    node.Next = null;
                    if (_head != null)
                        _head.Prev = null;
                    else
                        _tail = null;
                }

                _curEnumerator.Modified = true;
                Interlocked.Decrement(ref _count);
                Interlocked.Increment(ref _operations);
                return true;
            }
            finally
            {
#if DEBUG
                Interlocked.Decrement(ref _insaneExclusive);
#endif
                //node.lastOp = 2;
                _nodeHeap?.Return(node, deDup);
                _syncRoot.Release(Environment.TickCount, false);//FALSE
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
                await _syncRoot.WaitAsync().FastPath();

                var insane = _count * 2;
                var cur = _head;
                while (cur != null && insane --> 0)
                {
                    var tmp = cur.Next;
                    _nodeHeap.Return(cur);
                    cur = tmp;
                }

                _count = 0;
                _tail = _head = null;
            }
            finally
            {
                _syncRoot.Release(Environment.TickCount);
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
                await _syncRoot.WaitAsync().FastPath();
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
                _syncRoot.Release(Environment.TickCount);
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
