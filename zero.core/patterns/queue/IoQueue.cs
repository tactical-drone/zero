using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
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
            public IoZNode Next;
            public IoZNode Prev;
            public T Value;
            public int Qid;
#if DEBUG
            public long Id;
#endif
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

        private const int Qe = 16 << 2;

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
            
            var backPressure = concurrencyLevel;
            _nodeHeap = new IoHeap<IoZNode>(desc, capacity, static (_,_) => new IoZNode(), configuration.HasFlag(Mode.DynamicSize)) {
                PushAction = 
                node =>
                {
                    node.Next = null;
                    node.Prev = null;
                    node.Value = default;
                    Interlocked.Increment(ref node.Qid);
                },
                PopAction = (node, _) =>
                {
                    Interlocked.Increment(ref node.Qid);
                }
            };

            var c = configuration.HasFlag(Mode.DynamicSize) ? Environment.ProcessorCount << 3 : concurrencyLevel;
            IIoZeroSemaphoreBase<int> q = new IoZeroCore<int>(desc, c, _asyncTasks,1);
            _syncRoot = q.ZeroRef(ref q, _ => Environment.TickCount);

            if (configuration.HasFlag(Mode.Pressure))
            {
                _pressure = new IoZeroCore<int>($"qp {description}", c, _asyncTasks, zeroAsyncMode:false);
                _pressure.ZeroRef(ref _pressure, _ => Environment.TickCount);
            }
            
            if (configuration.HasFlag(Mode.BackPressure))
            {
                _backPressure = new IoZeroCore<int>($"qbp {description}", configuration.HasFlag(Mode.DynamicSize) ? Environment.ProcessorCount << 3 : backPressure + 1, _asyncTasks, configuration.HasFlag(Mode.DynamicSize) ? Environment.ProcessorCount << 3 : backPressure, false);
                _backPressure.ZeroRef(ref _backPressure, _ => Environment.TickCount);
            }

            _curEnumerator = new IoQueueEnumerator<T>(this);
        }

        #region memory
        private readonly string _description; 
        private int _zeroed;
        private readonly IIoZeroSemaphoreBase<int> _syncRoot;
        private readonly IIoZeroSemaphoreBase<int> _pressure;
        private readonly IIoZeroSemaphoreBase<int> _backPressure;
        private CancellationTokenSource _asyncTasks = new();
        private IoHeap<IoZNode> _nodeHeap;
        
        private IoZNode _head;
        private IoZNode _tail;
        private int _count;
        private long _operations;
        #endregion

        public string Description => $"{_description}, ops = {_operations}, count = {_count}, head = {_head}, tail = {_tail}, open = {_syncRoot.ReadyCount}, blocking = {_syncRoot.WaitCount}, z = {Zeroed}";
        public int Capacity => _nodeHeap.Capacity;
        public int Count => _count;
        public IoZNode Head => _head;
        public IoZNode Tail => _tail;

        public IoHeap<IoZNode> NodeHeap => _nodeHeap;
        public IIoZeroSemaphoreBase<int> Pressure => _backPressure;

        private IoQueueEnumerator<T> _curEnumerator;
        private readonly Mode _configuration;
        public Mode Configuration => _configuration;

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
                _syncRoot.Release(Environment.TickCount);
                if (zero)
                {
                    await ClearAsync().FastPath(); //TODO perf: can these two steps be combined?
                    await _nodeHeap.ZeroManagedAsync<object>().FastPath();

                    //Prime zero
                    if (!_asyncTasks.IsCancellationRequested)
                        _asyncTasks.Cancel();

                    _nodeHeap = null;
                    _count = 0;
                    _head = null;
                    _tail = null;

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
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public ValueTask<IoZNode> EnqueueAsync(T item) => EnqueueAsync<object>(item);

        /// <summary>
        /// Blocking enqueue at the back
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <param name="onAtomicAdd">Additional actions to perform in the critical area</param>
        /// <param name="context">atomic context</param>
        /// <returns>The queued item's linked list node</returns>
        public async ValueTask<IoZNode> EnqueueAsync<TC>(T item, Func<TC,ValueTask> onAtomicAdd = null, TC context = default)
        {
            if (_zeroed > 0 || item == null)
                return null;

            //wait on back pressure
            if (_backPressure != null && (await _backPressure.WaitAsync().FastPath()).ElapsedMs() == int.MaxValue || _zeroed > 0)
            {
                if(_zeroed == 0 && (!_backPressure?.Zeroed()??true))
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(EnqueueAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                return null;
            }
            
            var node = _nodeHeap.Take();

            if (node != null)
            {
                node.Value = item;

                await _syncRoot.WaitAsync().FastPath();
                try
                {
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
                        _tail.Next = node;
                        node.Prev = Interlocked.Exchange(ref _tail, node);
                    }

                    Interlocked.Increment(ref _count);
                    Interlocked.Increment(ref _operations);
                    return node;
                }
                finally
                {
                    if (!Zeroed)
                    {
                        //additional atomic actions
                        try
                        {
                            if (onAtomicAdd != null)
                            {
                                var ts = Environment.TickCount;
                                await onAtomicAdd.Invoke(context).FastPath();
                                if (ts.ElapsedMs() > Qe)
                                    LogManager.GetCurrentClassLogger().Warn($"q onAtomicAdd; t = {ts.ElapsedMs()}ms");
                            }
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
                            _pressure?.Release(Environment.TickCount, true);
                            _syncRoot.Release(Environment.TickCount, true);
                        }
                    }
                }
            }

            throw new OutOfMemoryException($"{nameof(EnqueueAsync)}: {_nodeHeap}");
        }

        /// <summary>
        /// Blocking enqueue item at the head of the Q
        /// </summary>
        /// <param name="item">The item to enqueue</param>
        /// <returns>The queued item node</returns>
        public async ValueTask<IoZNode> PushBackAsync(T item)
        {
            if (_zeroed > 0 || item == null)
                return null;

            //wait on back pressure
            if (_backPressure != null && (await _backPressure.WaitAsync().FastPath()).ElapsedMs() == int.MaxValue)
            {
                if (_zeroed == 0 && (!_backPressure?.Zeroed()??true))
                    LogManager.GetCurrentClassLogger().Fatal($"{nameof(PushBackAsync)}{nameof(_backPressure.WaitAsync)}: back pressure failure ~> {_backPressure}");
                return null;
            }

            var node = _nodeHeap.Take();

            if (node != null)
            {
                node.Value = item;

                await _syncRoot.WaitAsync().FastPath();
                try
                {
#if DEBUG
                    //int ii;
                    //Debug.Assert((ii = Interlocked.Increment(ref _insaneExclusive)) == 1 || _syncRoot.Zeroed(), $"{nameof(_insaneExclusive)} = {ii} > 1");
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
                        _head.Prev = node;
                        node.Next = Interlocked.Exchange(ref _head, node);
                    }

                    Interlocked.Increment(ref _count);
                    Interlocked.Increment(ref _operations);
                    return node;
                }
                finally
                {
                    if (!Zeroed)
                    {
#if DEBUG
                        Interlocked.Decrement(ref _insaneExclusive);
#endif
                        _pressure?.Release(Environment.TickCount, true);
                        _syncRoot.Release(Environment.TickCount, true);
                    }
                }
            }

            throw new OutOfMemoryException($"{_description} - ({_nodeHeap.Count} + {_nodeHeap.ReferenceCount})/{_nodeHeap.Capacity}, count = {_count}");
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

            T retVal = default;
            try
            {
                if (_pressure != null && (await _pressure.WaitAsync().FastPath()).ElapsedMs() == int.MaxValue)
                    return default;

                await _syncRoot.WaitAsync().FastPath();
                try
                {
#if DEBUG
                    //int ii;
                    //Debug.Assert((ii = Interlocked.Increment(ref _insaneExclusive)) == 1 || _syncRoot.Zeroed(), $"{nameof(_insaneExclusive)} = {ii} > 1, {nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount}, wait =  {_syncRoot.WaitCount}");
                    //Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed(), $"{nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount} [INVALID], wait =  {_syncRoot.WaitCount}");
                    //Debug.Assert(_insaneExclusive < 2 || _syncRoot.Zeroed() || Zeroed, $"{nameof(_insaneExclusive)}: {_insaneExclusive}");
                    Debug.Assert(Interlocked.Increment(ref _insaneExclusive) == 1 || _syncRoot.Zeroed());
                    Debug.Assert(_syncRoot.ReadyCount <= 0 || _syncRoot.Zeroed());
                    Debug.Assert(_insaneExclusive < 2);
#endif
                    if (_count == 0)
                        return default;

                    dq = Interlocked.Exchange(ref _head, _head.Next);

                    if (_head != null)
                        _head.Prev = null;
                    else
                        _tail = null;

                    Interlocked.Decrement(ref _count);
                    Interlocked.Increment(ref _operations);
                }
                finally
                {
                    if (!Zeroed)
                    {
#if DEBUG
                        Interlocked.Decrement(ref _insaneExclusive);
                        //Debug.Assert(_syncRoot.ReadyCount == 0, $"INVALID {nameof(_syncRoot.ReadyCount)} = {_syncRoot.ReadyCount}");
                        Debug.Assert(_syncRoot.ReadyCount == 0);
#endif
                        try
                        {
                            
                            

                            //DQ cost being load balanced
                            if (dq != null)
                            {
                                retVal = dq.Value;
                                _nodeHeap.Return(dq);
                            }

                            _backPressure?.Release(Environment.TickCount, true);
                            _syncRoot.Release(Environment.TickCount, false);
                        }
                        catch when (_zeroed > 0)
                        {
                        }
                        catch (Exception e) when (_zeroed == 0)
                        {
                            LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed!");
                        }
                    }
                }
            }
            catch when (_zeroed > 0) { }
            catch (Exception e) when (_zeroed == 0)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"{_description}: DQ failed! {nameof(_count)} = {_count}, {nameof(_tail)} = {_tail}, {nameof(_head)} = {_head}, heap => {_nodeHeap.Description}");
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
                        _tail = _tail.Prev;

                        if (_tail != null)
                            _tail.Next = null;
                        else
                            _head = null;
                    }
                }
                else
                {
                    Debug.Assert(_head == node || Zeroed);
                    
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
                _nodeHeap.Return(node, deDup);//TODO, up one?
                
                
                _backPressure?.Release(Environment.TickCount, true);
                _syncRoot.Release(Environment.TickCount, false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<IoZNode> GetEnumerator()
        {
            _curEnumerator = (IoQueueEnumerator<T>)_curEnumerator.Reuse(this, c => new IoQueueEnumerator<T>((IoQueue<T>)c));
            
            return _curEnumerator;
            //return _curEnumerator = new IoQueueEnumerator<T>(this);
            //return new IoQueueEnumerator<T>(this);
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

                Interlocked.Exchange(ref _count, 0);
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
            if (_curEnumerator is { Zeroed: false })
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
