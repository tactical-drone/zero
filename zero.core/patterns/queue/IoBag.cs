using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using NLog.LayoutRenderers;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.queue.enumerator;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.queue
{
    /// <summary>
    /// A lit Q with strong order guarantees 
    /// </summary>
    public class IoBag<T> : IEnumerable<T>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="capacity">The initial capacity</param>
        /// <param name="asyncTasks">When used as async blocking collection</param>
        /// <param name="concurrencyLevel">Max expected concurrency</param>
        /// <param name="zeroAsyncMode"></param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public IoBag(string description, int capacity, CancellationTokenSource asyncTasks = null, int concurrencyLevel = 1, bool zeroAsyncMode = false)
        {
#if DEBUG
            _description = description;
#else
            _description = string.Empty;
#endif

            _blockingCollection = asyncTasks != null;
            _capacity = capacity++;
            _storage = new T[capacity];
            _bloom = new int[capacity];

            if (_blockingCollection)
            {
                _fanSync = new IoZeroSemaphoreSlim(asyncTasks, $"fan {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning
                _balanceSync = new IoZeroSemaphoreSlim(asyncTasks, $"balance {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode, contextUnsafe:false); //TODO: tuning
                _zeroSync = new IoZeroSemaphoreChannel<T>($"pump  {description}", concurrencyLevel, zeroAsyncMode: zeroAsyncMode); //TODO: tuning

                _fanSyncs = Enumerable.Repeat<AsyncDelegate>(BlockOnConsumeAsync, concurrencyLevel).ToArray();
                _balanceSyncs = Enumerable.Repeat<AsyncDelegate>(BalanceOnConsumeAsync, concurrencyLevel).ToArray();
                _zeroSyncs = Enumerable.Repeat<AsyncDelegate>(PumpOnConsumeAsync, concurrencyLevel).ToArray();
            }
        }

        #region packed
        private long _head;
        private readonly string _description;

        private readonly T[]   _storage;
        private readonly int[] _bloom;

        private readonly IoZeroSemaphoreSlim _fanSync;
        private readonly IoZeroSemaphoreChannel<T> _zeroSync;
        private readonly IoZeroSemaphoreSlim _balanceSync;
        private readonly AsyncDelegate[] _fanSyncs;
        private readonly AsyncDelegate[] _balanceSyncs;
        private readonly AsyncDelegate[] _zeroSyncs;
        private long _tail;
        private delegate IAsyncEnumerable<T> AsyncDelegate();

        private readonly bool _blockingCollection;

        private readonly int _capacity;
        private int _zeroed;
        private int _clearing;
        private int _blockingConsumers;
        private int _sharingConsumers;
        private int _pumpingConsumers;
        private int _count;
        #endregion

        public long Tail => Interlocked.Read(ref _tail);
        public long Head => Interlocked.Read(ref _head);

        /// <summary>
        /// ZeroAsync status
        /// </summary>
        public bool Zeroed => _zeroed > 0;

        /// <summary>
        /// Description
        /// </summary>
        public string Description => $"{nameof(IoBag<T>)}: z = {_zeroed > 0}, {nameof(Count)} = {_count}/{Capacity}, h = {Head}/{Tail}({Head%Capacity}/{Tail % Capacity}) (max: {Capacity}) (d:{Tail - Head}), desc = {_description}";

        /// <summary>
        /// Current number of items in the bag
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Capacity
        /// </summary>
        public long Capacity => _capacity;

        /// <summary>
        /// Q item by index
        /// </summary>
        /// <param name="idx">index</param>
        /// <returns>Object stored at index</returns>
        public T this[long idx]
        {
#if !DEBUG
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
            get
            {
                Debug.Assert(idx >= 0);
                Interlocked.MemoryBarrier();
                return _storage[idx % _capacity];
            }
#if !DEBUG
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
            set
            {
                Debug.Assert(idx >= 0);
                _storage[idx % _capacity] = value;
                Interlocked.MemoryBarrier();
            }
        }

        /// <summary>
        /// Add item to the bag
        /// </summary>
        /// <param name="item">The item to be added</param>
        /// <param name="deDup">Whether to de-dup this item from the bag</param>
        /// <param name="onAtomicAdd">Action to execute on add success</param>
        /// <param name="context">Action context</param>
        /// <exception cref="OutOfMemoryException">Thrown if we are internally OOM</exception>
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long TryEnqueue<TC>(T item, bool deDup = false, Action<TC> onAtomicAdd = null, TC context = default)
        {
            Debug.Assert(Zeroed || item != null);
            retry:
            if (Zeroed || _clearing > 0 || _count >= Capacity)
                return -1;

            if (deDup)
            {
                if (Contains(item))
                    return -1;
            }

            try
            {
                //message pump
                if (_blockingCollection && _pumpingConsumers > 0)
                {
                    try
                    { 
                        _zeroSync.Release(item);
                        return 0;
                    }
                    catch
                    {
                        // ignored
                    }
                }

                long latch;
                var next = _tail.ZeroNext(latch = Head + Capacity);
                if (next < latch)
                {
                    if (next < Tail - Capacity)//slow thread resurrections are retried, not ideal but nothing you can do
                        goto retry;

                    var spinWait = new SpinWait();
                    
                    ref var fastBloom = ref _bloom[next % Capacity];

                    while (fastBloom != 0)
                    {
                        if (Zeroed)
                            return -1;

                        spinWait.SpinOnce();
                    }

                    if (Interlocked.CompareExchange(ref fastBloom, 1, 0) == 0)
                    {
                        Interlocked.Increment(ref _count);
                        this[next] = item;
                        Interlocked.Exchange(ref fastBloom, 2);
                    }
                    else if (next < Tail - Capacity)
                    {
                        goto retry;
                    }
                    else if(!Zeroed)
                        throw new InvalidOperationException($"{nameof(TryEnqueue)}: Control should never reach here; next = {next}({next%Capacity}), bloom = {fastBloom}, {Description}");
                }
                else
                    return -1;

                //execute atomic action on success
                onAtomicAdd?.Invoke(context);
                
                //service async blockers
                if (_blockingCollection && _sharingConsumers > 0)
                {
                    try
                    {
                        _balanceSync.Release(Environment.TickCount);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                if (_blockingCollection && _blockingConsumers > 0)
                {
                    try
                    {
                        _fanSync.Release(Environment.TickCount, _blockingConsumers);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                //_curEnumerator.IncIteratorCount(); //TODO: is this a good idea?

                return next;
            }
            catch when (Zeroed)
            {
            }
            catch (Exception e) when (!Zeroed)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }

            return -1;
        }
#if !DEBUG
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public long TryEnqueue(T item, bool deDup = false) => TryEnqueue<object>(item, deDup);

        /// <summary>
        /// Try take from the Q, round robin
        /// </summary>
        /// <param name="slot">The item to be fetched</param>
        /// <returns>True if an item was found and returned, false otherwise</returns>
#if DEBUG
    [MethodImpl(MethodImplOptions.NoInlining)]
#else
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public bool TryDequeue([MaybeNullWhen(false)] out T slot)
        {
            try
            {
                retry:
                if (Count == 0 || Zeroed)
                {
                    slot = default;
                    return false;
                }

                long latch;
                var next = _head.ZeroNext(latch = Tail);
                
                if (next < latch) 
                {
                    if (next < Tail - Capacity)//slow thread resurrections are retried, not ideal but nothing you can do
                        goto retry;

                    var spinWait = new SpinWait();
                    ref var fastBloom = ref _bloom[next % Capacity];

                    while (fastBloom != 2)
                    {
                        if (Zeroed)
                        {
                            slot = default;
                            return false;
                        }

                        spinWait.SpinOnce();
                    }

                    //if (Interlocked.Exchange(ref _bloom[idx], 0) == 2)
                    int prev;
                    if((prev = Interlocked.CompareExchange(ref fastBloom, 3, 2)) == 2)
                    {
                        Interlocked.Decrement(ref _count);
                        slot = this[next];
                        this[next] = default;

                        Interlocked.Exchange(ref fastBloom, 0);
                        return true;
                    }

                    if (next < Tail - Capacity)//slow threads
                        goto retry;

                    if (!Zeroed)
                        throw new InvalidOperationException($"{nameof(TryDequeue)}[SET]: Control should never reach here; next = {next}({next % Capacity}), bloom = {fastBloom}, was = {prev}, {Description}");
                }
            }
            catch (Exception e)
            {
                LogManager.LogFactory.GetCurrentClassLogger().Error(e, $"{nameof(TryDequeue)} failed!");
            }
            
            slot = default;
            return false;
        }

        /// <summary>
        /// Peeks the head of the queue
        /// </summary>
        /// <param name="result">Returns the head of the Q</param>
        /// <returns>True if the head was not null, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek([MaybeNullWhen(false)] out T result)
        {
            return (result = this[Head]) != null;
        }

        /// <summary>
        /// ZeroAsync managed cleanup
        /// </summary>
        /// <param name="op">Optional callback to execute on all items in the bag</param>
        /// <param name="nanite">Callback context</param>
        /// <param name="zero">Whether the bag is assumed to contain <see cref="IIoNanite"/>s and should only be zeroed out</param>
        /// <typeparam name="TC">The callback context type</typeparam>
        /// <returns>True if successful, false if something went wrong</returns>
        /// <exception cref="ArgumentException">When zero is true but <see cref="nanite"/> is not of type <see cref="IIoNanite"/></exception>
        public async ValueTask<bool> ZeroManagedAsync<TC>(Func<T, TC, ValueTask> op = null, TC nanite = default, bool zero = false)
        {
            if (zero && Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return true;

            if (Interlocked.CompareExchange(ref _clearing, 1, 0) != 0)
                return true;

            try
            {
                for (long i = 0; i < Capacity; i++)
                {
                    var item = this[i];
                    try
                    {
                        if (item is not null)
                        {
                            if (op != null)
                                await op(item, nanite).FastPath();

                            if (item is IIoNanite ioNanite)
                            {
                                if (!ioNanite.Zeroed())
                                    await ioNanite.DisposeAsync((IIoNanite)nanite, string.Empty).FastPath();
                            }
                            else if (item is IAsyncDisposable asyncDisposable)
                                await asyncDisposable.DisposeAsync().FastPath();
                            else if (item is IDisposable disposable)
                                disposable.Dispose();
                        }
                    }
                    catch (InvalidCastException) { }
                    catch (Exception) when (Zeroed) { }
                    catch (Exception e) when (!Zeroed)
                    {
                        LogManager.GetCurrentClassLogger().Trace(e, $"{_description}: {op}, {item}, {nanite}");
                    }
                    finally
                    { 
                        this[i] = default;
                    }
                }

            }
            catch
            {
                return false;
            }
            finally
            {
                _count = (int)(_head = _tail = 0);
                Interlocked.Exchange(ref _clearing, 0);
            }

            return true;
        }

        /// <summary>
        /// Contains
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(T item)
        {
            for (var i = _head; i < _tail; i++)
            {
                var index = i % Capacity;
                try
                {
                    if (_bloom[index] > 0 && (_storage[index]?.Equals(item)??false))
                        return true;
                }
                catch
                {
                    // ignored
                }

                //Console.WriteLine($"Comparing {_storage[i % Capacity].ToString()} - {item.ToString()}");
            }

            return false;
        }

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            //_curEnumerator = (IoQEnumerator<T>)_curEnumerator.Reuse(this, b => new IoQEnumerator<T>((IoZeroQ<T>)b));
            //return _curEnumerator;
            //return _curEnumerator = new IoQEnumerator<T>(this);
            return new IoBagEnumerator<T>(this);
        }

        /// <summary>
        /// Async blocking consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        protected async IAsyncEnumerable<T> BlockOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return default;

            try
            {
                Interlocked.Increment(ref _blockingConsumers);
                var cur = Head;
                while (!_fanSync.Zeroed())
                {
                    if (cur >= Tail && (await _fanSync.WaitAsync().FastPath()).ElapsedMs() > 0x7ffffff)
                        break;

                    var newItem = this[cur];
                    if (newItem != null)
                        yield return newItem;

                    cur++;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _blockingConsumers);
            }
        }

        /// <summary>
        /// Async balancing consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        protected async IAsyncEnumerable<T> BalanceOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return default;

            try
            {
                Interlocked.Increment(ref _sharingConsumers);

                //follow the tail
                while (!_balanceSync.Zeroed())
                {
                    try
                    {
                        if (Count == 0 && (await _balanceSync.WaitAsync().FastPath()).ElapsedMs() > 0x7ffffff)
                            break;
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e, Description);
                    }

                    while (TryDequeue(out var next))
                        yield return next;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _sharingConsumers);
            }
        }

        /// <summary>
        /// Async pump consumer support
        /// </summary>
        /// <returns>The next inserted item</returns>
        protected async IAsyncEnumerable<T> PumpOnConsumeAsync()
        {
            if (!_blockingCollection)
                yield return default;

            try
            {
                Interlocked.Increment(ref _pumpingConsumers);

                //follow the tail
                while (!_zeroSync.Zeroed())
                {
                    T next = default;
                    try
                    {
                        if (Count == 0 || !TryDequeue(out next))
                        {
                            if((next = await _zeroSync.WaitAsync().FastPath()) == null)
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        LogManager.GetCurrentClassLogger().Error(e,Description);
                    }
                    yield return next;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _pumpingConsumers);
            }
        }
        public IAsyncEnumerable<T> BlockOnConsumeAsync(int threadIndex) => _fanSyncs[threadIndex]();
        public IAsyncEnumerable<T> BalanceOnConsumeAsync(int threadIndex) =>_balanceSyncs[threadIndex]();
        public IAsyncEnumerable<T> PumpOnConsumeAsync(int threadIndex) => _zeroSyncs[threadIndex]();

        /// <summary>
        /// Returns the bag enumerator
        /// </summary>
        /// <returns>The bag enumerator</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}