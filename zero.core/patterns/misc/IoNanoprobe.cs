using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// ZeroAsync teardown
    /// </summary>
    [AttributeUsageAttribute(AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Property | AttributeTargets.Field)]
    public class IoNanoprobe : Attribute, IIoNanite, IDisposable
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
            
        }

        /// <summary>
        /// Used as destruction sentinels
        /// </summary>
        /// <param name="reason">The teardown reason</param>
        public IoNanoprobe(string reason)
        {
            Description = reason;
        }

        /// <summary>
        /// Constructs a nano probe
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="concurrencyLevel">Maximum blockers allowed. Consumption: 128 bits per tick.</param>
        protected IoNanoprobe(string description, int concurrencyLevel)
        {
            Description = description ?? GetType().Name;

            _concurrencyLevel = concurrencyLevel;

            _zeroHive = new LinkedList<IoZeroSub>();
            _zeroHiveMind = new LinkedList<IIoNanite>();

            _zeroed = 0;
            ZeroedFrom = default;
            TearDownTime = default;
            CascadeTime = default;
            Uptime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTasks = new CancellationTokenSource();
            
            var enableFairQ = true;
            var enableDeadlockDetection = true;
#if RELEASE
            enableDeadlockDetection = false;
#endif

            _nanoMutex = new IoZeroSemaphore(description, maxBlockers: _concurrencyLevel * 2, initialCount: 1, maxAsyncWork:0, enableAutoScale: false, enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);
            _nanoMutex.ZeroRef(ref _nanoMutex, AsyncTasks.Token);
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
        private static readonly ILogger _logger;

        /// <summary>
        /// seeds UIDs
        /// </summary>
        private static ulong _uidSeed;


        /// <summary>
        /// Hive Teardown maximum elasticity allowed 
        /// </summary>
        private const int MaxElasticity = 10000;
        
        /// <summary>
        /// Used for equality compares
        /// </summary>
        private ulong _zId;

        /// <summary>
        /// returns UID
        /// </summary>
        public ulong SerialNr => _zId;

        /// <summary>
        /// Description
        /// </summary>
        public virtual string Description { get; }

        /// <summary>
        /// Sync root
        /// </summary>
        private IIoZeroSemaphore _nanoMutex;

        /// <summary>
        /// Who zeroed this object
        /// </summary>
        public IIoNanite ZeroedFrom { get; private set; }

        /// <summary>
        /// Measures how long teardown takes
        /// </summary>
        public long TearDownTime;

        /// <summary>
        /// Measures how long cascading takes
        /// </summary>
        public long CascadeTime;

        /// <summary>
        /// Uptime
        /// </summary>
        public readonly long Uptime;
        
        /// <summary>
        /// Are we zeroed?
        /// </summary>
        private volatile int _zeroed;

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
        private LinkedList<IoZeroSub> _zeroHive;

        /// <summary>
        /// All subscriptions
        /// </summary>
        //private IoQueue<IIoNanite> _zeroHiveMind;
        private LinkedList<IIoNanite> _zeroHiveMind;

        /// <summary>
        /// Max number of blockers
        /// </summary>
        private readonly int _concurrencyLevel;
        
        /// <summary>
        /// A secondary constructor for async stuff
        /// </summary>
        public virtual ValueTask<bool> ConstructAsync() {return new ValueTask<bool>(true);}
        
        /// <summary>
        /// Teardown termination sentinel
        /// </summary>
        private static readonly IoNanoprobe Sentinel = new  IoNanoprobe("self");
        
        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        public void Dispose()
        {
            ZeroAsync(Sentinel).AsTask().GetAwaiter();
        }

        /// <summary>
        /// ZeroAsync
        /// </summary>
        public virtual async ValueTask<bool> ZeroAsync(IIoNanite from)
        {
#if DEBUG
            if (from == null)
            {
                throw new NullReferenceException(nameof(from));
            }
#endif
            if (_zeroed > 0)
                return true;

            ZeroedFrom ??= !from.Equals(this) ? from : Sentinel;

            await ZeroOptionAsync(static async @this =>
            {
                await @this.ZeroAsync(true).FastPath().ConfigureAwait(false);    
            }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
            GC.SuppressFinalize(this);
            return true;
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
        public bool Zeroed()
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
        public ValueTask<LinkedListNode<IoZeroSub>> ZeroSubAsync<T>(Func<IIoNanite, T, ValueTask<bool>> sub,
            T closureState = default,
            [CallerFilePath] string filePath = null, [CallerMemberName] string memberName = null,
            [CallerLineNumber] int lineNumber = default)
        {
            var newSub = new IoZeroSub($"{Path.GetFileName(filePath)}:{memberName} line {lineNumber}").SetAction(sub, closureState);
            
            return ValueTask.FromResult(_zeroHive.AddFirst(newSub));
        }

        /// <summary>
        /// Unsubscribe to a zero event
        /// </summary>
        /// <param name="sub">The original subscription</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> UnsubscribeAsync(LinkedListNode<IoZeroSub> sub)
        {
            _zeroHive.Remove(sub);
            return ValueTask.FromResult(true);
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

            _zeroHiveMind.AddFirst(target);

            if (twoWay) //zero
                await target.ZeroHiveAsync(this).FastPath().ConfigureAwait(false);

            return (target, true);
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        private async ValueTask ZeroAsync(bool disposing)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;
            
            CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            try
            {
                if(!(AsyncTasks?.IsCancellationRequested??false))
                    AsyncTasks?.Cancel();
            }
            catch (Exception) when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
#if DEBUG
                _logger.Trace(e, $"{Description}: Cancel async tasks failed!!!");
#endif
            }
            
            //Dispose managed
            if (disposing)
            {

                foreach (var zeroSub in _zeroHive)
                {
                    if (!await zeroSub.ExecuteAsync(this).FastPath().ConfigureAwait(false))
                        _logger.Error($"{zeroSub?.From} - zero sub {((IIoNanite)zeroSub?.Target)?.Description} on {Description} returned with errors!");
                }

                foreach (var zeroSub in _zeroHiveMind)
                {
                    await zeroSub.ZeroAsync(this).FastPath().ConfigureAwait(false);
                }

                CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - CascadeTime;
                TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                try
                {
                    await ZeroManagedAsync().FastPath().ConfigureAwait(false);
                }
                catch (Exception e)
                {
#if DEBUG
                    _logger.Error(e, $"[{this}] {nameof(ZeroManagedAsync)} returned with errors!");
#endif
                }
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
            catch (Exception e)
            {
#if DEBUG
                _logger.Error(e, $"ZeroAsync [Un]managed errors: {Description}");
#endif
            }

#if DEBUG
            if (_extracted < 2 && disposing)
            {
                throw new ApplicationException($"{Description}: BUG!!! Memory leaks detected in type {GetType().Name}!!!");
            }
#endif

            TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TearDownTime;
            //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
            //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");

            try
            {
                if (Uptime.ElapsedSec() > 10 && CascadeTime > TearDownTime * 2 && TearDownTime > 0)
                    _logger.Fatal(
                        $"{GetType().Name}:Z/{Description}> SLOW TEARDOWN!, c = {CascadeTime:0.0}ms, t = {TearDownTime:0.0}ms, count = {_zeroHive.Count}");
            }
            catch
            {
                // ignored
            }
        }

        /// <summary>
        /// Cancellation token source
        /// </summary>
        public CancellationTokenSource AsyncTasks { get; protected set; }

        /// <summary>
        /// Manages unmanaged objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void ZeroUnmanaged()
        {
            _nanoMutex = null;
            _zeroHive = null;
            _zeroHiveMind = null;
#if DEBUG
            Interlocked.Increment(ref _extracted);
#endif
        }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        public virtual ValueTask ZeroManagedAsync()
        {
            _nanoMutex.Zero();
            _zeroHive.Clear();
            _zeroHiveMind.Clear();
#if DEBUG
            Interlocked.Increment(ref _extracted);
#endif
            return ValueTask.CompletedTask;
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

        ///  <summary>
        ///  Ensures that a ownership transfer action is synchronized
        ///  </summary>
        ///  <param name="ownershipAction">The ownership transfer callback</param>
        ///  <param name="userData"></param>
        ///  <param name="disposing">If this call is inside a disposing thread</param>
        ///  <param name="force">Forces the action regardless of zero state</param>
        ///  <returns>true if ownership was passed, false otherwise</returns>
        public virtual async ValueTask<bool> ZeroAtomicAsync<T>(Func<IIoNanite, T, bool, ValueTask<bool>> ownershipAction,
            T userData = default,
            bool disposing = false, bool force = false)
        {
            try
            {
                //Prevents strange things from happening
                if (_zeroed > 0 && !force)
                    return false;

                try
                {
                    if (!force)
                    {
                        //lock (_nanoMutex)
                        try
                        {
                            if (await _nanoMutex.WaitAsync().FastPath().ConfigureAwait(false))
                            {
                                return (_zeroed == 0) &&
                                       await ownershipAction(this, userData, disposing).FastPath().ConfigureAwait(false);
                            }
                        }
                        catch (Exception e)
                        {
                            if(!Zeroed())
                                _logger.Error(e, $"{Description}: Unable to ensure action {ownershipAction}, target = {ownershipAction.Target}");
                            return false;
                        }
                        finally
                        {
                            await _nanoMutex.ReleaseAsync().FastPath().ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        return await ownershipAction(this, userData, disposing).FastPath().ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    _logger.Fatal(e, $"{Description}");
                    // ignored
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e);
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"Unable to ensure ownership in {Description}");
            }

            return false;
        }

        /// <summary>
        /// Async execution options. <see cref="ZeroAsync"/> needs trust, but verify...
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
        protected async ValueTask ZeroAsync<T>(Func<T,ValueTask> continuation, T state, CancellationToken asyncToken, TaskCreationOptions options, TaskScheduler scheduler = null, bool unwrap = false, [CallerFilePath] string filePath = null,[CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default )
        {
            var nanite = state as IoNanoprobe;
            try
            {
                var zeroAsyncTask = Task.Factory.StartNew(static async nanite =>
                    {
                        var (@this, action, state, fileName, methodName, lineNumber, scheduler) =
                            (ValueTuple<IoNanoprobe, Func<T, ValueTask>, T, string, string, int, TaskScheduler>)nanite;

                        var nanoprobe = state as IoNanoprobe;
                        try
                        {
                            await action(state).FastPath().ConfigureAwait(false);
                        }
                        catch (TaskCanceledException e) when ( nanoprobe != null && !nanoprobe.Zeroed() ||
                                                   nanoprobe == null && @this._zeroed == 0)
                        {
#if DEBUG
                            _logger.Trace(e,$"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(ZeroAsync)}");
#endif
                        }
                        catch (NullReferenceException e) when ( nanoprobe != null && !nanoprobe.Zeroed() ||
                                                               nanoprobe == null && @this._zeroed == 0)
                        {
#if DEBUG
                            _logger.Trace(e,$"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(ZeroAsync)}");
#endif
                        }
                        catch (Exception e) when ( nanoprobe != null && !nanoprobe.Zeroed() ||
                                                   nanoprobe == null && @this._zeroed == 0)
                        {
                            _logger.Error(e,$"{Path.GetFileName(fileName)}:{methodName}() line {lineNumber} - [{@this.Description}]: {nameof(ZeroAsync)}");
                        }
                    }, ValueTuple.Create(this, continuation, state, filePath, methodName, lineNumber, scheduler),
                    asyncToken, options, scheduler ?? TaskScheduler.Current);
                
                if (unwrap)
                    await zeroAsyncTask.Unwrap();
                
                await zeroAsyncTask;
            }
            catch (TaskCanceledException e) when (nanite != null && !nanite.Zeroed()
                                                  || nanite == null)
            {
                if(nanite != null)
                    _logger.Error(e, Description);
            }
            catch(TaskCanceledException) when (nanite != null && nanite.Zeroed()){}
            catch(Exception) when (nanite != null && nanite.Zeroed()){}
            catch (Exception e) when (nanite != null && !nanite.Zeroed()
                                      || nanite == null)
            {
                throw ZeroException.ErrorReport(this, $"{nameof(ZeroAsync)} returned with errors!", e);
            }
        }

        /// <summary>
        /// Async execution options. <see cref="ZeroAsync"/> needs trust, but verify...
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
        protected ValueTask ZeroAsync<T>(Func<T,ValueTask> continuation, T state, TaskCreationOptions options, bool unwrap = false, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default )
        {
            return ZeroAsync(continuation, state, AsyncTasks.Token, options, TaskScheduler.Default, unwrap, filePath, methodName: methodName, lineNumber);
        }

        /// <summary>
        /// Async execution options. <see cref="ZeroAsync"/> needs trust, but verify...
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
                    unwrap, filePath, methodName: methodName, lineNumber);
            }
            catch (Exception) when(Zeroed()){ return ValueTask.CompletedTask;}
            catch (Exception e)when (!Zeroed())
            {
                return ValueTask.FromException(e);//TODO why am I doing it this way?
            }
        }

        /// <summary>
        /// Async execution options. <see cref="ZeroAsync"/> needs trust, but verify...
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
        public LinkedList<IIoNanite> ZeroHiveMind()
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
            return SerialNr == other.SerialNr;
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