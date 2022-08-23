using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Base class for async/await only frameworks (useful for example if you need to call async/await code from a critical section)
    /// 
    /// Adds memory management dependency framework options to any object. Allows objects build hierarchies that cascade teardown etc.
    ///
    /// Tracks concurrency level supported, because this async/await framework bakes load balancing parameters into every object that is super useful.
    ///
    /// Additionally, contains useful debug stuff like descriptions & uptime etc.
    /// </summary>
    public class IoNanoprobe : IIoNanite, IDisposable
    {
        /// <summary>
        /// static constructor
        /// </summary>
        static IoNanoprobe()
        {
            Volatile.Write(ref ZeroRoot, ZeroSyncRoot(short.MaxValue>>1, new CancellationTokenSource()));
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Empty constructor
        /// </summary>
        protected IoNanoprobe()
        {
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTasks = new CancellationTokenSource();
        }

        /// <summary>
        /// Constructs a nano probe
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="concurrencyLevel">Maximum blockers allowed</param>
        protected IoNanoprobe(string description, int concurrencyLevel)
        {
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTasks = new CancellationTokenSource();
#if DEBUG
            Description = description ?? GetType().Name;
#else
            Description = string.Empty;
#endif

            _concurrencyLevel = concurrencyLevel <= 0 ? 1 : concurrencyLevel;

            //TODO: tuning
#if DEBUG
            _zeroHive = new IoQueue<IoZeroSub>($"{nameof(_zeroHive)} {description}", 4, _concurrencyLevel, IoQueue<IoZeroSub>.Mode.DynamicSize);
            _zeroHiveMind = new IoQueue<IIoNanite>($"{nameof(_zeroHiveMind)} {description}", 4, _concurrencyLevel, IoQueue<IIoNanite>.Mode.DynamicSize);
#else
            _zeroHive = new IoQueue<IoZeroSub>(string.Empty, 4, _concurrencyLevel, IoQueue<IoZeroSub>.Mode.DynamicSize);
            _zeroHiveMind = new IoQueue<IIoNanite>(string.Empty, 4, _concurrencyLevel, IoQueue<IIoNanite>.Mode.DynamicSize);
#endif

            //Volatile.Write(ref ZeroRoot, ZeroSyncRoot(concurrencyLevel, AsyncTasks));

            Interlocked.Exchange(ref UpTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~IoNanoprobe()
        {
            try
            {
                ZeroDisposeAsync(false).AsTask().GetAwaiter();
            }
            catch (Exception e)
            {
                _logger.Fatal(e);
            }
        }

        /// <summary>
        /// A cheap logger
        /// </summary>
        // ReSharper disable once InconsistentNaming
        private static readonly ILogger _logger;

        /// <summary>
        /// seeds UIDs
        /// </summary>
        private static long _uidSeed;

        /// <summary>
        /// Used for equality compares
        /// </summary>
        private readonly long _zId;

        /// <summary>
        /// returns UID
        /// </summary>
        public long Serial => _zId;

        /// <summary>
        /// Description
        /// </summary>
        public virtual string Description { get; }

        /// <summary>
        /// Who zeroed this object
        /// </summary>
        public IIoNanite ZeroedFrom { get; private set; }

        /// <summary>
        /// Explain why teardown happened
        /// </summary>
        public string ZeroReason { get; private set; }

        /// <summary>
        /// Measures how long teardown takes
        /// </summary>
        public int TearDownTime;

        /// <summary>
        /// Measures how long cascading takes
        /// </summary>
        public int CascadeTime;

        /// <summary>
        /// UpTime
        /// </summary>
        public readonly long UpTime;

        /// <summary>
        /// Are we zero primed? Before we disposed of a hierarchy, we prime it with a light weight signal (so that thread spinners can start unwinding, etc)
        /// </summary>
        private volatile int _zeroPrimed;

        /// <summary>
        /// Are we zeroed?
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// Are we disposed?
        /// </summary>
        private volatile int _disposed;

#if DEBUG
        /// <summary>
        /// Have are there any leaks?
        /// </summary>
        private volatile int _extracted;
#endif

        /// <summary>
        /// All subscriptions to be called on teardown
        /// </summary>
        private IoQueue<IoZeroSub> _zeroHive;

        /// <summary>
        /// Contains teardown hierarchies. When this object is zeroed, so will be the objects in this queue;
        /// </summary>
        private IoQueue<IIoNanite> _zeroHiveMind;

        /// <summary>
        /// Concurrency level supported by this object, if applicable.
        /// </summary>
        private readonly int _concurrencyLevel;

        /// <summary>
        /// Initialize concurrency level critical region
        /// </summary>
        /// <param name="concurrencyLevel"></param>
        /// <param name="asyncTasks"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static IIoZeroSemaphoreBase<int> ZeroSyncRoot(int concurrencyLevel, CancellationTokenSource asyncTasks)
        {
            IIoZeroSemaphoreBase<int> z = new IoZeroCore<int>(string.Empty, Math.Min(20 + concurrencyLevel * 40, short.MaxValue / 3), asyncTasks,1);
            z.ZeroRef(ref z, _ => Environment.TickCount);
            return z;
        }

        /// <summary>
        /// Dispose pattern from non async contexts, else use <see cref="DisposeAsync"/>
        /// </summary>
        public void Dispose()
        {
            //Console.WriteLine("Z");
            _ = Task.Factory.StartNew(static async state =>
            {
                var @this = (IoNanoprobe)state;
                await @this.ZeroDisposeAsync(false).FastPath();
            },this, CancellationToken.None,TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }

        /// <summary>
        /// Tracks teardown
        /// </summary>
        private static long _zCount;

        /// <summary>
        /// root sync, used to generate critical regions for async contexts
        /// </summary>
        private static readonly IIoZeroSemaphoreBase<int> ZeroRoot;

        /// <summary>
        /// ZeroDisposeAsync
        /// </summary>
        public ValueTask DisposeAsync(IIoNanite @from, string reason, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return default;

            ZeroedFrom = from;
#if RELEASE
            ZeroReason = $"ZERO: {reason??"N/A"}"; 
#else
            ZeroReason = $"{methodName}:{lineNumber} - reason = {reason ?? "N/A"}";
#endif

            IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
            {
                var @this = (IoNanoprobe)state;

                //prime for garbage collection (threads stop spinning)
                try
                {
                    if(@this.Serial % 2 == 0 || IoZeroScheduler.Zero == null || !IoZeroScheduler.Zero.Fork(@this.ZeroPrime))
                        @this.ZeroPrime();
                }
                catch (Exception e)
                {
                    _logger.Trace(e, $"{@this.Description}");
                }

                //collect memory
                await @this.ZeroDisposeAsync(true).FastPath();
            }, this);

            if (Interlocked.Increment(ref _zCount) % 100000 == 0)
            {
                Console.WriteLine("z");
            }

            return default;
        }

        /// <summary>
        /// Prime for zero
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void ZeroPrime()
        {
            if (_zeroPrimed > 0 || Interlocked.CompareExchange(ref _zeroPrimed, 1, 0) != 0)
                return;

            if (_zeroHiveMind == null) return;
            
            foreach (var ioZNode in _zeroHiveMind)
            {
                try
                {
                    if (ioZNode.Value != null && !ioZNode.Value.Zeroed())
                        IoZeroScheduler.Zero.Fork(ioZNode.Value.ZeroPrime);
                }
                catch
                {
                    // ignored
                }
            }
        }

        /// <summary>
        /// Debug teardown path printout
        /// </summary>
        private void PrintPathToZero()
        {
            var builder = new StringBuilder();
            var cur = ZeroedFrom;
            while (cur?.Zeroed() ?? false)
            {
                builder.Append($"/> {cur.GetType().Name}({cur.Description})");
                if (cur == this)
                    break;
                cur = cur.ZeroedFrom;
            }

            _logger.Trace($"[{GetType().Name}]{Description}: ZEROED from: {(!string.IsNullOrEmpty(builder.ToString()) ? builder.ToString() : "this")}");
        }

        /// <summary>
        /// Indicate zero status, the best feature of this base class.
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual bool Zeroed()
        {
            return _zeroPrimed > 0 || _zeroed > 0 || AsyncTasks.IsCancellationRequested;
        }

        /// <summary>
        /// Subscribe to zero event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <param name="closureState">Closure state</param>
        /// <param name="filePath"></param>
        /// <param name="memberName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>The handler</returns>        
        public ValueTask<IoQueue<IoZeroSub>.IoZNode> ZeroSubAsync<T>(Func<IIoNanite, T, ValueTask<bool>> sub,
            T closureState = default,
            [CallerFilePath] string filePath = null, [CallerMemberName] string memberName = null,
            [CallerLineNumber] int lineNumber = default)
        {
            try
            {
                if (Zeroed())
                    return default;

                var newSub = new IoZeroSub($"zero sub> {Path.GetFileName(filePath)}:{memberName} line {lineNumber}").SetAction(sub, closureState);
                return _zeroHive.EnqueueAsync(newSub);                
            }
            catch (Exception) when(Zeroed()) { }
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"{nameof(ZeroSubAsync)} returned with errors");
            }

            return default;
        }

        /// <summary>
        /// Unsubscribe to a zero event
        /// </summary>
        /// <param name="sub">The original subscription</param>
        public ValueTask<bool> UnsubscribeAsync(IoQueue<IoZeroSub>.IoZNode sub)
        {
            return Zeroed() ? default : _zeroHive.RemoveAsync(sub, sub.Qid);
        }

        /// <summary>
        /// Cascade zero events to <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">Enforces mutual zero</param>
        public async ValueTask<(T target, bool success, IoQueue<IIoNanite>.IoZNode sub)> ZeroHiveAsync<T>(T target,
            bool twoWay = false) where T : IIoNanite
        {
            if (_zeroed > 0 || target == null)
                return (default, false, null);

            var zNode = await _zeroHiveMind.EnqueueAsync(target).FastPath();

            if (zNode == null)
            {
                if(_zeroed > 0)
                    throw new ApplicationException($"{nameof(ZeroHiveAsync)}: {nameof(_zeroHiveMind.EnqueueAsync)} failed!, cap =? {_zeroHiveMind.Count}/{_zeroHiveMind.Capacity}");
                return (default, false, null);
            }

            if (twoWay) //zero
            {
                var sub = (await target.ZeroHiveAsync(this).FastPath()).sub;

                if (sub != null)
                {
                    await ZeroSubAsync(static async (_, state) =>
                    {
                        var (_, target, sub) = state;
                        var qId = sub.Qid;
                        try
                        {
                            if(!target.Zeroed())
                                await target.ZeroHiveMind().RemoveAsync(sub, qId).FastPath();
                        }
                        catch
                        {
                            // ignored
                        }

                        return true;
                    }, (this, target, sub)).FastPath();
                }
            }

            return (target, true, zNode);
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        private async ValueTask ZeroDisposeAsync(bool disposing)
        {
            // Only once
            if (_disposed > 0 || Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;

            CascadeTime = Environment.TickCount;
            var desc = Description;

            //Dispose managed
            if (disposing)
            {
                if (_zeroHive != null)
                {
                    while(await _zeroHive.DequeueAsync().FastPath() is { } zeroSub)
                    {
                        if(zeroSub.Executed) continue;
                        
                        if (!await zeroSub.ExecuteAsync(this).FastPath())
                            _logger.Trace($"{zeroSub.From} - zero sub {((IIoNanite)zeroSub.Target)?.Description} on {Description} returned with errors!");
                    }
                }

                if (_zeroHiveMind != null)
                {
                    while (await _zeroHiveMind.DequeueAsync().FastPath() is { } zeroSub)
                        await zeroSub.DisposeAsync(this, $"[ZERO CASCADE] from {desc}").FastPath();
                }
                
                CascadeTime = CascadeTime.ElapsedMs();
                TearDownTime = Environment.TickCount;

                try
                {
                    await ZeroManagedAsync().FastPath();
                }
#if DEBUG
                catch (Exception e) when(!Zeroed())
                {

                    _logger.Error(e, $"[{this}] {nameof(ZeroManagedAsync)} returned with errors!");
                }
#else
                catch when (Zeroed()){}
#endif
            }

            //Dispose unmanaged
            try
            {
                ZeroUnmanaged();
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"[{this}] {nameof(ZeroManagedAsync)} returned with errors!");
            }

            //Dispose async task cancellation token registrations etc.
            try
            {
                if (!(AsyncTasks?.IsCancellationRequested ?? true) && AsyncTasks.Token.CanBeCanceled)
                    AsyncTasks.Cancel(false);
                AsyncTasks?.Dispose();
            }
#if DEBUG
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"ZeroDisposeAsync [Un]managed errors: {Description}");
            }
#else
            catch
            {
                // ignored
            }
#endif

#if DEBUG
            if (_extracted < 2 && disposing)
            {
                throw new ApplicationException($"{Description}: BUG!!! Memory leaks detected in type {GetType().Name}!!!");
            }
#endif
            TearDownTime = TearDownTime.ElapsedMs();

            try
            {
                if (UpTime.ElapsedUtcMs() > 10 && CascadeTime > TearDownTime * 7 && CascadeTime > 20000)
                    _logger.Fatal($"{GetType().Name}:Z/{Description}> SLOW TEARDOWN!, c = {CascadeTime:0.0}ms, t = {TearDownTime:0.0}ms");
            }
            catch
            {
                // ignored
            }

            GC.SuppressFinalize(this);
#if TRACE
            _logger.Trace($"z:{Serial}> {desc}; from = {ZeroedFrom?.Description}, reason = `{reason}'");
#endif
            ZeroedFrom = null;
            ZeroRoot.ZeroSem();
        }

        /// <summary>
        /// Cancellation token source
        /// </summary>
        public readonly CancellationTokenSource AsyncTasks;

        /// <summary>
        /// Manages unmanaged objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            ZeroedFrom = null;
            _zeroHive = null;
            _zeroHiveMind = null;
#if !DEBUG
            ZeroReason = null;
#endif
            ZeroedFrom = null;
            //ZeroRoot = null;
#endif

#if DEBUG
            Interlocked.Increment(ref _extracted);
#endif
        }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        public virtual ValueTask ZeroManagedAsync()
        {
#if DEBUG
            Interlocked.Increment(ref _extracted);
#endif
            return default;
        }

        /// <summary>
        /// Returns the concurrency level
        /// </summary>
        /// <returns>The concurrency level</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ZeroConcurrencyLevel()
        {
            return _concurrencyLevel;
        }

        /// <summary>
        /// Execute atomic actions
        /// </summary>
        /// <typeparam name="T">callback context</typeparam>
        /// <param name="ownershipAction">The callback</param>
        /// <param name="userData">context</param>
        /// <param name="disposing">if we are disposing</param>
        /// <param name="force">Force execution even if zeroed</param>
        /// <returns>Ownership action result, true if success false otherwise</returns>
        public async ValueTask<bool> ZeroAtomicAsync<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction,
            T userData = default, bool disposing = false, bool force = false)
        {
            try
            {
                await ZeroRoot.WaitAsync().FastPath();

                //insane checks
                if (_zeroed > 0 && !force)
                    return false;

                try
                {
                    if (!force)
                        return _zeroed == 0 && await ownershipAction(this, userData, disposing).FastPath();
                    else
                        return await ownershipAction(this, userData, disposing).FastPath();
                }
                catch when (Zeroed())
                {
                }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Error(e, $"{Description}: Unable to ensure action {ownershipAction}, target = {ownershipAction.Target}");
                }
            }
            catch (TaskCanceledException){}
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"Unable to ensure ownership in {Description}");
            }
            finally
            {
#if DEBUG
                //TODO: we moved it here, the one at the top works but needs Interlocked.MemoryBarrierProcessWide();
                //Debug.Assert(Zeroed() || ZeroRoot.Zeroed() || ZeroRoot.ReadyCount == 0, $"{nameof(ZeroRoot)}: [FAILED], ReadyCount = {ZeroRoot.ReadyCount}, wait = {ZeroRoot.WaitCount}");
                Debug.Assert(Zeroed() || ZeroRoot.Zeroed() || ZeroRoot.ReadyCount == 0);
#endif
                ZeroRoot.Release(Environment.TickCount, true);
            }

            return false;
        }

        /// <summary>
        /// Executes a async continuation in a critical section
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="options">Task options</param>
        /// <param name="unwrap">If the task awaited should be unwrapped, effectively making this a blocking call</param>
        /// <param name="scheduler">The scheduler</param>
        /// <returns>A ValueTask</returns>
        protected async ValueTask ZeroAsync<T>(Func<T, ValueTask> continuation, T state, TaskCreationOptions options = TaskCreationOptions.DenyChildAttach, TaskScheduler scheduler = null, bool unwrap = false)
        {
            var nanite = state as IoNanoprobe ?? this;
            try
            {
                var zeroAsyncTask = Task.Factory.StartNew(static async nanite =>
                {
                    var (@this, action, state) = (ValueTuple<IoNanoprobe, Func<T, ValueTask>, T>)nanite;

                    var nanoprobe = state as IoNanoprobe;
                    try
                    {
                        await action(state).FastPath();
                    }
#if DEBUG
                    //catch (TaskCanceledException e) when ( nanoprobe != null && !nanoprobe.Zeroed() ||
                    //                           nanoprobe == null && @this._zeroed == 0)
                    //{
                    //    _logger.Trace(e,$"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(DisposeAsync)}");
                    //}
#else
                    catch (TaskCanceledException) { }
#endif
                    catch when (nanoprobe != null && nanoprobe.Zeroed() || nanoprobe == null && @this._zeroed > 0) { }
                    catch (Exception e) when (nanoprobe != null && !nanoprobe.Zeroed() || nanoprobe == null && @this._zeroed == 0)
                    {
                        //_logger.Error(e, $"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(DisposeAsync)}");
                        _logger.Error(e, $"[{@this.Description}]: {nameof(DisposeAsync)}");
                    }
                }, (this, continuation, state), CancellationToken.None, options, scheduler??IoZeroScheduler.ZeroDefault);
                
                if (unwrap)
                    await zeroAsyncTask.Unwrap();
            }
            catch (TaskCanceledException e) when (!nanite.Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException) when (nanite.Zeroed()) { }
            catch (Exception) when (nanite.Zeroed()) { }
            catch (Exception e) when (!nanite.Zeroed())
            {
                throw ZeroException.ErrorReport(this, $"{nameof(DisposeAsync)} returned with errors!", e);
            }
        }

        /// <summary>
        /// Base exception, not really used atm. 
        /// </summary>
        public class ZeroException:ApplicationException
        {
            private ZeroException(string message, Exception innerException)
            :base(message, innerException)
            {
                
            }

            /// <summary>
            /// Create a new exception
            /// </summary>
            /// <param name="state">Extra info</param>
            /// <param name="message">The error message</param>
            /// <param name="exception">The inner Exception</param>
            /// <param name="filePath">The calling method file name</param>
            /// <param name="methodName">The calling method name</param>
            /// <param name="lineNumber">The calling method line number</param>
            /// <returns>The new exception</returns>
            public static ZeroException ErrorReport(object state, string message = null, Exception exception = null, [CallerMemberName] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default )
            {
#if DEBUG
                if (state == null)
                    throw new NullReferenceException($"{nameof(state)}");
#endif
                
                var nanite = state as IoNanoprobe;
                var description = nanite?.Description ?? $"{state}";
                
                return new ZeroException($"{Path.GetFileName(filePath)}:{methodName}() {lineNumber} - [{description}]: {message}", exception);
            }
        }


        /// <summary>
        /// Returns the hive mind subscriptions
        /// </summary>
        /// <returns>The hive</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoQueue<IoZeroSub> ZeroHive()
        {
            return _zeroHive;
        }

        /// <summary>
        /// Returns the hive mind targets to be zeroed when this object is zeroed
        /// </summary>
        /// <returns>The hive</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoQueue<IIoNanite> ZeroHiveMind()
        {
            return _zeroHiveMind;
        }
        
        /// <summary>
        /// Equals, useful for debugging
        /// </summary>
        /// <param name="other"></param>
        /// <returns>True if equal, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(IIoNanite other)
        {
            if (other == null)
                return false;

            return Serial == other.Serial;
        }

        /// <summary>
        /// A description of this object
        /// </summary>
        /// <returns>A description</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            try
            {
                return Description??GetHashCode().ToString();
            }
            catch
            {
                return GetHashCode().ToString();
            }
        }
    }
}