using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// ZeroAsync teardown
    /// </summary>
    public class IoNanoprobe : IIoNanite, IDisposable
    {
        /// <summary>
        /// static constructor
        /// </summary>
        static IoNanoprobe()
        {
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
        /// Used as destruction sentinels
        /// </summary>
        /// <param name="reason">The teardown reason</param>
        public IoNanoprobe(string reason)
        {
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTasks = new CancellationTokenSource();
            Description = reason;
        }
        
        /// <summary>
        /// Constructs a nano probe
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="concurrencyLevel">Maximum blockers allowed. Consumption: 128 bits per tick.</param>
        protected IoNanoprobe(string description, int concurrencyLevel)
        {
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTasks = new CancellationTokenSource();
#if DEBUG
            Description = description ?? GetType().Name;
#else 
            Description = "";
#endif

            _concurrencyLevel = concurrencyLevel <= 0 ? 1 : concurrencyLevel;

#if DEBUG
            _zeroHive = new IoQueue<IoZeroSub>($"{nameof(_zeroHive)} {description}", 16, _concurrencyLevel, autoScale:true);
            _zeroHiveMind = new IoQueue<IIoNanite>($"{nameof(_zeroHiveMind)} {description}", 16, _concurrencyLevel, autoScale:true);
#else
            _zeroHive = new IoQueue<IoZeroSub>(string.Empty, 16, _concurrencyLevel, autoScale:true);
            _zeroHiveMind = new IoQueue<IIoNanite>(string.Empty, 16, _concurrencyLevel, autoScale: true);
#endif

            _zeroRoot = new IoZeroSemaphore(string.Empty, concurrencyLevel, 1, 0);
            _zeroRoot.ZeroRef(ref _zeroRoot, AsyncTasks);

            UpTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~IoNanoprobe()
        {
#pragma warning disable 4014
            try
            {
                ZeroAsync(false).AsTask().GetAwaiter();
            }
            catch (Exception e)
            {
                _logger.Fatal(e);
            }
#pragma warning restore 4014
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
        /// Continue On Captured Context
        /// </summary>
        public static bool ContinueOnCapturedContext => true;

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
        public long TearDownTime;

        /// <summary>
        /// Measures how long cascading takes
        /// </summary>
        public long CascadeTime;

        /// <summary>
        /// UpTime
        /// </summary>
        public readonly long UpTime;
        
        /// <summary>
        /// Are we zero primed?
        /// </summary>
        private volatile int _zeroPrimed;

        /// <summary>
        /// Are we zeroed?
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// Are we zeroed?
        /// </summary>
        private volatile int _zeroedSec;

#if DEBUG
        /// <summary>
        /// Have are there any leaks?
        /// </summary>
        private volatile int _extracted;
#endif

        /// <summary>
        /// All subscriptions
        /// </summary>
        //private IoQueue<IoZeroSub> _zeroHive;
        private volatile IoQueue<IoZeroSub> _zeroHive;

        /// <summary>
        /// All subscriptions
        /// </summary>
        //private IoQueue<IIoNanite> _zeroHiveMind;
        private volatile IoQueue<IIoNanite> _zeroHiveMind;

        /// <summary>
        /// Max number of blockers
        /// </summary>
        private readonly int _concurrencyLevel;

        /// <summary>
        /// A secondary constructor for async stuff
        /// </summary>
        /// <param name="localContext"></param>
        public virtual ValueTask<bool> ConstructAsync(object localContext = null) {return new ValueTask<bool>(true);}
        
        /// <summary>
        /// config await
        /// </summary>
        public bool Zc => ContinueOnCapturedContext;
        
        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        public void Dispose()
        {
            Zero(this, $"{nameof(IDisposable)}");
        }

        /// <summary>
        /// Tracks teardown
        /// </summary>
        private static long _zCount;

        /// <summary>
        /// root sync
        /// </summary>
        private IIoZeroSemaphore _zeroRoot;

        /// <summary>
        /// ZeroAsync
        /// </summary>
        public void Zero(IIoNanite @from, string reason)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;

            ZeroedFrom = from;
            ZeroReason = $"ZERO: {reason??"N/A"}";

#pragma warning disable CS4014
            ZeroAsync(static async @this =>
            {
                //prime garbage
                await @this.ZeroPrimeAsync().FastPath().ConfigureAwait(false);
                await @this.ZeroAsync(true).FastPath().ConfigureAwait(false);
            }, this, default,TaskCreationOptions.DenyChildAttach);
#pragma warning restore CS4014

            if (Interlocked.Increment(ref _zCount) % 100000 == 0)
            {
                Console.WriteLine("z");
            }
        }

        /// <summary>
        /// Prime for zero
        /// </summary>
        public async ValueTask ZeroPrimeAsync()
        {
            if (_zeroPrimed > 0 || Interlocked.CompareExchange(ref _zeroPrimed, 1, 0) != 0)
                return;

            try
            {
                if (!(AsyncTasks?.IsCancellationRequested ?? true) && AsyncTasks.Token.CanBeCanceled)
                    AsyncTasks.Cancel(false);
            }
            catch (Exception e)
            {
                _logger.Trace(e);
            }

            if (_zeroHiveMind != null)
            {
                foreach (var ioZNode in _zeroHiveMind)
                {
                    if (!ioZNode.Value.Zeroed())
                        await ioZNode.Value.ZeroPrimeAsync().FastPath().ConfigureAwait(Zc);
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

            _logger.Trace(
                $"[{GetType().Name}]{Description}: ZEROED from: {(!string.IsNullOrEmpty(builder.ToString()) ? builder.ToString() : "this")}");
        }

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual bool Zeroed()
        {
            return _zeroed > 0 || AsyncTasks.IsCancellationRequested;
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
            return Zeroed() ? default : _zeroHive.RemoveAsync(sub);
        }

        /// <summary>
        /// Cascade zero events to <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">Enforces mutual zero</param>
        public async ValueTask<(T target, bool success)> ZeroHiveAsync<T>(T target, bool twoWay = false) where T : IIoNanite
        {
            if (_zeroed > 0)
                return (default, false);

            var zNode = await _zeroHiveMind.EnqueueAsync(target).FastPath().ConfigureAwait(Zc);

            if (zNode == null)
            {
                if(_zeroed > 0)
                    throw new ApplicationException($"{nameof(ZeroHiveAsync)}: {nameof(_zeroHiveMind.EnqueueAsync)} failed!, cap =? {_zeroHiveMind.Count}/{_zeroHiveMind.Capacity}");
                return (default, false);
            }
            
            if (twoWay) //zero
                await target.ZeroHiveAsync(this).FastPath().ConfigureAwait(Zc);
            else
            {
                await ZeroSubAsync((_, @this) =>
                {
                    @this._zeroHiveMind.RemoveAsync(zNode).FastPath().ConfigureAwait(Zc);
                    return new ValueTask<bool>(true);
                }, this).FastPath().ConfigureAwait(Zc);
            }
            
            return (target, true);
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        private async ValueTask ZeroAsync(bool disposing)
        {
            // Only once
            if (_zeroedSec > 0 || Interlocked.CompareExchange(ref _zeroedSec, 1, 0) != 0)
                return;

            CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var desc = Description;
#if DEBUG
            var reason = ZeroReason;
#endif
            //Dispose managed
            if (disposing)
            {
                if (_zeroHive != null)
                {
                    IoZeroSub zeroSub;
                    while((zeroSub = await _zeroHive.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
                    {
                        if (!zeroSub.Executed && !await zeroSub.ExecuteAsync(this).FastPath().ConfigureAwait(Zc))
                            _logger.Error($"{zeroSub.From} - zero sub {((IIoNanite)zeroSub.Target)?.Description} on {Description} returned with errors!");
                    }

                    await _zeroHive.ZeroManagedAsync<object>(zero:true).FastPath().ConfigureAwait(Zc);
                    _zeroHive = null;
                }

                if (_zeroHiveMind != null)
                {
                    IIoNanite zeroSub;
                    while ((zeroSub = await _zeroHiveMind.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
                    {
                        if (zeroSub.Zeroed()) continue;

                        zeroSub.Zero(this, $"[ZERO CASCADE] teardown from {desc}");

                        //throttle teardown so it floods in breadth
                        await Task.Delay(32).ConfigureAwait(Zc);
                    }

                    await _zeroHiveMind.ZeroManagedAsync<object>(zero: true).FastPath().ConfigureAwait(Zc);
                    _zeroHiveMind = null;
                }
                
                CascadeTime = CascadeTime.ElapsedMs();
                TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                try
                {
                    await ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
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
                AsyncTasks?.Dispose();
            }
#if DEBUG
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"ZeroAsync [Un]managed errors: {Description}");
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
            //if (UpTime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
            //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");

            try
            {
                if (UpTime.ElapsedMs() > 10 && CascadeTime > TearDownTime * 7 && CascadeTime > 20000)
                    _logger.Fatal($"{GetType().Name}:Z/{Description}> SLOW TEARDOWN!, c = {CascadeTime:0.0}ms, t = {TearDownTime:0.0}ms");
            }
            catch
            {
                // ignored
            }

            GC.SuppressFinalize(this);
            ZeroedFrom = null;
#if DEBUG
            _logger.Trace($"#{Serial} ~> {desc}: Reason: `{reason}'");
#endif
        }
        /// <summary>
        /// Cancellation token source
        /// </summary>
        public CancellationTokenSource AsyncTasks { get; private set; }

        /// <summary>
        /// Manages unmanaged objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void ZeroUnmanaged()
        {
#if SAFE_RELEASE
            AsyncTasks = null;
            ZeroedFrom = null;
            _zeroHive = null;
            _zeroHiveMind = null;
#if !DEBUG
            ZeroReason = null;
#endif
            ZeroedFrom = null;
            _zeroRoot = null;
#endif

#if DEBUG
            Interlocked.Increment(ref _extracted);
#endif
        }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        public virtual async ValueTask ZeroManagedAsync()
        {
            _zeroRoot.ZeroSem();

            if (_zeroHiveMind != null)
                await _zeroHiveMind.ZeroManagedAsync<object>(zero:true).FastPath().ConfigureAwait(Zc);
            if (_zeroHive != null)
                await _zeroHive.ZeroManagedAsync<object>(zero: true).FastPath().ConfigureAwait(Zc);
#if DEBUG
            Interlocked.Increment(ref _extracted);
#endif
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

        public async ValueTask<bool> ZeroAtomic<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction,
            T userData = default, bool disposing = false, bool force = false)
        {
            try
            {
                if (!await _zeroRoot.WaitAsync().FastPath().ConfigureAwait(Zc))
                {
                    return false;
                }

                //Prevents strange things from happening
                if (_zeroed > 0 && !force)
                    return false;

                try
                {
                    if (!force)
                    {
                        try
                        {
                            return _zeroed == 0 && await ownershipAction(this, userData, disposing).FastPath().ConfigureAwait(Zc);

                        }
                        catch when (Zeroed())
                        {
                        }
                        catch (Exception e) when (!Zeroed())
                        {
                            _logger.Error(e,
                                $"{Description}: Unable to ensure action {ownershipAction}, target = {ownershipAction.Target}");
                            return false;
                        }
                    }
                    else
                    {
                        return await ownershipAction(this, userData, disposing).FastPath().ConfigureAwait(Zc);
                    }
                }
                catch when (Zeroed())
                {
                }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Error(e, $"{Description}");
                    // ignored
                }
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"Unable to ensure ownership in {Description}");
            }
            finally
            {
                _zeroRoot.Release();
            }

            return false;
        }

        /// <summary>
        /// Async execution options. <see cref="Zero"/> needs trust, but verify...
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="asyncToken">The async cancellation token</param>
        /// <param name="options">Task options</param>
        /// <param name="scheduler">The scheduler</param>
        /// <param name="filePath"></param>
        /// <param name="methodName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>A ValueTask</returns>
        protected async ValueTask<TResult> ZeroAsync<T,TResult>(Func<T, ValueTask<TResult>> continuation, T state, CancellationToken asyncToken, TaskCreationOptions options, TaskScheduler scheduler = null, [CallerFilePath] string filePath = null,[CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default )
        {
            var nanite = state as IoNanoprobe??this;
            try
            {
                var zeroAsyncTask = Task.Factory.StartNew<ValueTask<TResult>>(static async nanite =>
                {
                    var (@this, action, state, fileName, methodName, lineNumber) = (ValueTuple<IoNanoprobe, Func<T, ValueTask<TResult>>, T, string, string, int>)nanite;

                    var nanoprobe = state as IoNanoprobe;
                    try
                    {
                        return await action(state).FastPath().ConfigureAwait(@this.Zc);
                    }
#if DEBUG
                    catch (TaskCanceledException e) when ( nanoprobe != null && !nanoprobe.Zeroed() ||
                                               nanoprobe == null && @this._zeroed == 0)
                    {
                        _logger.Trace(e,$"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(Zero)}");
                    }
#else
                    catch (TaskCanceledException) { }
#endif
                    catch when (nanoprobe != null && nanoprobe.Zeroed() ||
                                nanoprobe == null && @this._zeroed > 0)
                    { }
                    catch (Exception e) when (nanoprobe != null && !nanoprobe.Zeroed() ||
                                              nanoprobe == null && @this._zeroed == 0)
                    {
                        _logger.Error(e, $"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(Zero)}");
                    }

                    return default;
                }, ValueTuple.Create(this, continuation, state, filePath, methodName, lineNumber), asyncToken, options, TaskScheduler.Default);

                return await zeroAsyncTask.Result;
                
            }
            catch (TaskCanceledException e) when (!nanite.Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch(TaskCanceledException) when (nanite.Zeroed()){}
            catch(Exception) when (nanite.Zeroed()){}
            catch (Exception e) when (!nanite.Zeroed())
            {
                throw ZeroException.ErrorReport(this, $"{nameof(Zero)} returned with errors!", e);
            }

            return default;
        }

        /// <summary>
        /// Async execution options. <see cref="Zero"/> needs trust, but verify...
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="asyncToken">The async cancellation token</param>
        /// <param name="options">Task options</param>
        /// <param name="unwrap">If the task awaited should be unwrapped, effectively making this a blocking call</param>
        /// <param name="scheduler">The scheduler</param>
        /// <param name="filePath"></param>
        /// <param name="methodName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>A ValueTask</returns>
        protected async ValueTask ZeroAsync<T>(Func<T, ValueTask> continuation, T state, CancellationToken asyncToken, TaskCreationOptions options, TaskScheduler scheduler = null, bool unwrap = false, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default)
        {
            var nanite = state as IoNanoprobe ?? this;
            try
            {
                var zeroAsyncTask = Task.Factory.StartNew(static async nanite =>
                {
                    var (@this, action, state, fileName, methodName, lineNumber) = (ValueTuple<IoNanoprobe, Func<T, ValueTask>, T, string, string, int>)nanite;

                    var nanoprobe = state as IoNanoprobe;
                    try
                    {
                        await action(state).FastPath().ConfigureAwait(@this.Zc);
                    }
#if DEBUG
                        catch (TaskCanceledException e) when ( nanoprobe != null && !nanoprobe.Zeroed() ||
                                                   nanoprobe == null && @this._zeroed == 0)
                        {
                            _logger.Trace(e,$"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(Zero)}");
                        }
#else
                    catch (TaskCanceledException) { }
#endif
                    catch when (nanoprobe != null && nanoprobe.Zeroed() ||
                               nanoprobe == null && @this._zeroed > 0)
                    { }
                    catch (Exception e) when (nanoprobe != null && !nanoprobe.Zeroed() ||
                                              nanoprobe == null && @this._zeroed == 0)
                    {
                        _logger.Error(e, $"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(Zero)}");
                    }


                }, ValueTuple.Create(this, continuation, state, filePath, methodName, lineNumber), asyncToken, options, TaskScheduler.Default);

                if (unwrap)
                    await zeroAsyncTask.Unwrap();

                await zeroAsyncTask.ConfigureAwait(Zc);
            }
            catch (TaskCanceledException e) when (!nanite.Zeroed())
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException) when (nanite.Zeroed()) { }
            catch (Exception) when (nanite.Zeroed()) { }
            catch (Exception e) when (!nanite.Zeroed())
            {
                throw ZeroException.ErrorReport(this, $"{nameof(Zero)} returned with errors!", e);
            }
        }

        /// <summary>
        /// Async execution options. <see cref="Zero"/> needs trust, but verify...
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="options">Task options</param>
        /// <param name="scheduler">The scheduler</param>
        /// <param name="filePath"></param>
        /// <param name="methodName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>A ValueTask</returns>
        protected ValueTask<TResult> ZeroAsync<T,TResult>(Func<T, ValueTask<TResult>> continuation, T state, TaskCreationOptions options, TaskScheduler scheduler = null,  [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default )
        {
            return ZeroAsync(continuation, state, AsyncTasks.Token, options, scheduler??TaskScheduler.Default, filePath, methodName: methodName, lineNumber);
        }

        /// <summary>
        /// Async execution options. <see cref="Zero"/> needs trust, but verify...
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="options">Task options</param>
        /// <param name="scheduler">The scheduler</param>
        /// <param name="filePath"></param>
        /// <param name="methodName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>A ValueTask</returns>
        protected ValueTask ZeroAsync<T>(Func<T, ValueTask> continuation, T state, TaskCreationOptions options, TaskScheduler scheduler = null, bool unwrap = false, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default)
        {
            
            return ZeroAsync<T>(continuation, state, AsyncTasks.Token, options, scheduler ?? TaskScheduler.Default, unwrap, filePath, methodName: methodName, lineNumber);
        }

        /// <summary>
        /// Async execution options. <see cref="Zero"/> needs trust, but verify...
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="options">Task options</param>
        /// <param name="unwrap"></param>
        /// <param name="scheduler">The scheduler</param>
        /// <param name="filePath"></param>
        /// <param name="methodName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>A ValueTask</returns>
        protected ValueTask ZeroOptionAsync<T>(Func<T, ValueTask> continuation, T state, TaskCreationOptions options, TaskScheduler scheduler = null, bool unwrap = true, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default)
        {
            try
            {
                return ZeroAsync(continuation, state, AsyncTasks.Token, options, scheduler ?? TaskScheduler.Default,
                    unwrap, filePath, methodName, lineNumber);
            }
            catch (Exception) when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{Description}, c = {continuation}, s = {state}, {Path.GetFileName(filePath)}:{methodName} line {lineNumber}");
            }
            return default;
        }

        /// <summary>
        /// Async execution options. <see cref="Zero"/> needs trust, but verify...
        /// </summary>
        /// <param name="continuation">The continuation</param>
        /// <param name="state">user state</param>
        /// <param name="asyncToken">The async cancellation token</param>
        /// <param name="options">Task options</param>
        /// <param name="unwrap">If the task awaited should be unwrapped, effectively making this a blocking call</param>
        /// <param name="scheduler">The scheduler</param>
        /// <param name="filePath"></param>
        /// <param name="methodName"></param>
        /// <param name="lineNumber"></param>
        /// <returns>A ValueTask</returns>
        protected ValueTask ZeroOptionAsync<T>(Func<T, ValueTask> continuation, T state,
            CancellationToken asyncToken, TaskCreationOptions options, TaskScheduler scheduler = null,
            bool unwrap = true, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null,
            [CallerLineNumber] int lineNumber = default)
        {
            return ZeroOptionAsync(continuation, state, options, scheduler ?? TaskScheduler.Current, unwrap, filePath, methodName, lineNumber);
        }

        public class ZeroException:ApplicationException
        {
            private ZeroException(string message, Exception innerException)
            :base(message, innerException)
            {
                
            }

            /// <summary>
            /// Create a new exceptions
            /// </summary>
            /// <param name="state">Extra info</param>
            /// <param name="message">The error message</param>
            /// <param name="exception">The inner Exception</param>
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
        /// Returns the hive mind
        /// </summary>
        /// <returns>The hive</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IoQueue<IIoNanite> ZeroHiveMind()
        {
            return _zeroHiveMind;
        }
        
        /// <summary>
        /// Equals
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