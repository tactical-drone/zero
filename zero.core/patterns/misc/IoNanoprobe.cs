using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
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

            _zeroSubs = new ConcurrentBag<IoZeroSub>();
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
            ZeroAsync(false); //.GetAwaiter().GetResult();
#pragma warning restore 4014
        }

        /// <summary>
        /// 
        /// </summary>
        private static readonly ILogger _logger;

        /// <summary>
        /// seeds UIDs
        /// </summary>
        private static ulong _uidSeed;

        /// <summary>
        /// Used for equality compares
        /// </summary>
        private ulong _zId;

        /// <summary>
        /// returns UID
        /// </summary>
        public ulong NpId => _zId;

        /// <summary>
        /// Description
        /// </summary>
        public virtual string Description { get; private set; }

        //private IoHeap<TMutex> _mutHeap;

        /// <summary>
        /// Sync root
        /// </summary>
        //private object _nanoMutex = 305;
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

        // /// <summary>
        // /// Used by superclass to manage all async calls
        // /// </summary>
        // //public CancellationTokenSource AsyncTasks { get; private set; } = new CancellationTokenSource();
        // public CancellationToken ZeroToken;

        /// <summary>
        /// Are we zeroed?
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// Are we disposed
        /// </summary>
        private volatile bool _disposed;

        /// <summary>
        /// Are we disposed
        /// </summary>
        public bool Disposed => _disposed;

        /// <summary>
        /// All subscriptions
        /// </summary>
        private ConcurrentBag<IoZeroSub> _zeroSubs;

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
        /// Teardown termination sentinel
        /// </summary>
        private static readonly IoNanoprobe DisposeSentinel = new  IoNanoprobe($"{nameof(IDisposable.Dispose)}");

        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        public void Dispose()
        {
#pragma warning disable 4014
            var z =  ZeroAsync(true).FastPath().ConfigureAwait(false);
#pragma warning restore 4014
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// ZeroAsync
        /// </summary>
        public virtual async ValueTask ZeroAsync(IIoNanite from)
        {
#if DEBUG
            if (from == null)
            {
                throw new NullReferenceException(nameof(from));
            }
#endif
            ZeroedFrom ??= !from.Equals(this) ? from : Sentinel;
            
            if (_zeroed > 0)
                return;
            
            //ZeroedFrom ??= !@from.Equals(this) ? @from : new IoNanoprobe("sentinel");
            
            //ZeroedFrom = !@from.Equals(this) ? @from : new IoNanoprobe("self");

            await ZeroAsyncOption(static async @this =>
            {
                await @this.ZeroAsync(true).FastPath().ConfigureAwait(false);    
            }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach).ConfigureAwait(false);
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
            return _disposed || _zeroed > 0 || AsyncTasks.IsCancellationRequested;
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
        public IoZeroSub ZeroSubscribe<T>(Func<IIoNanite, T, ValueTask> sub, T closureState = default,
            [CallerFilePath] string filePath = null, [CallerMemberName] string memberName = null,
            [CallerLineNumber] int lineNumber = default)
        {
            if (_zeroSubs == null)
                return null;
                    
            IoZeroSub newSub;
            try
            {
                newSub = new IoZeroSub($"{Path.GetFileName(filePath)}:{memberName} line {lineNumber}").SetAction(sub, closureState);
                _zeroSubs.Add(newSub);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            //regular cleanups
            //TODO tuning
            const int bufSize = 25;
            const double cleanThreshold = 0.33;
            if (_zeroSubs.Count > bufSize && _zeroSubs.Count(zeroSub => !zeroSub.Schedule) > bufSize * cleanThreshold)
                _zeroSubs = new ConcurrentBag<IoZeroSub>(_zeroSubs.Where(z => z.Schedule).ToList());

            return newSub;
        }

        /// <summary>
        /// Unsubscribe to a zero event
        /// </summary>
        /// <param name="sub">The original subscription</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unsubscribe(IoZeroSub sub)
        {
            
        }

        /// <summary>
        /// Cascade zero events to <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">Enforces mutual zero</param>
        public (T target, bool success) ZeroOnCascade<T>(T target, bool twoWay = false) where T : IIoNanite
        {
            if (_zeroed > 0)
                return (default, false);

            ZeroSubscribe(static async (from,target) => await target.ZeroAsync(from).FastPath().ConfigureAwait(false), target);

            if (twoWay) //zero
                target.ZeroSubscribe(static async (from,state) => await state.Item1.ZeroAsync(state.Item2).FastPath().ConfigureAwait(false), ValueTuple.Create(this, target));
            
            return (target, true);
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)] 
        private async ValueTask ZeroAsync(bool disposing)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) != 0)
                return;
            
            CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            try
            {
                if(!AsyncTasks.IsCancellationRequested)
                    AsyncTasks.Cancel();
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Debug(e, $"{Description}: Cancel async tasks failed!!!");
            }

            //emit zero event
            foreach(var zeroSub in _zeroSubs)
            {
                try
                {
                    if (!zeroSub.Schedule)
                        continue;

                    await zeroSub.ExecuteAsync(this).FastPath().ConfigureAwait(false);
                }
                //catch (NullReferenceException e)
                //{
                //    _logger.Trace(e, @this.Description);
                //}
                catch (Exception e) when (!Zeroed())
                {

                    try
                    {
                        _logger.Fatal(e, $"{zeroSub?.From} - zero sub {((IIoNanite) zeroSub?.Target)?.Description} on {Description} returned with errors!");
                    }
                    catch
                    {
                        // ignored
                    }
                }
                finally
                {
                    Unsubscribe(zeroSub);
                }
            }

            CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - CascadeTime;
            TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (disposing)
            {
                //Dispose managed
                try
                {
                    await ZeroManagedAsync().FastPath().ConfigureAwait(false);
                }
                //catch (NullReferenceException) { }
                catch (Exception e) when(!Zeroed())
                {
                    _logger.Error(e, $"[{this}] {nameof(ZeroManagedAsync)} returned with errors!");
                }
            }
            
            //Dispose unmanaged
            try
            {
                ZeroUnmanaged();
            }
            catch (Exception e)
            {
                _logger.Error(e, $"[{this}] {nameof(ZeroManagedAsync)} returned with errors!");
            }
            
            //Dispose async task cancellation token registrations etc.
            try
            {
                AsyncTasks.Dispose();
            }
            catch (NullReferenceException)
            {
            }
            catch (Exception e)
            {
                _logger.Error(e, $"ZeroAsync [Un]managed errors: {Description}");
            }

            AsyncTasks = null;

            TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TearDownTime;
            //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
            //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");

            try
            {
                if (Uptime.ElapsedSec() > 10 && CascadeTime > TearDownTime * 2 && TearDownTime > 0)
                    _logger.Fatal(
                        $"{GetType().Name}:Z/{Description}> SLOW TEARDOWN!, c = {CascadeTime:0.0}ms, t = {TearDownTime:0.0}ms, count = {_zeroSubs.Count}");
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
            _zeroSubs = null;
            _disposed = true;
        }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ValueTask ZeroManagedAsync()
        {
            _nanoMutex.Zero();
            _zeroSubs.Clear();
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
                            _nanoMutex.Release();
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
            return ZeroAsync(continuation, state, AsyncTasks.Token, options, TaskScheduler.Current, unwrap, filePath, methodName: methodName, lineNumber);
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
        protected ValueTask ZeroAsyncOption<T>(Func<T, ValueTask> continuation, T state, TaskCreationOptions options, TaskScheduler scheduler = null, bool unwrap = true, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default)
        {
            return ZeroAsync(continuation, state, AsyncTasks.Token, options, scheduler??TaskScheduler.Default, unwrap, filePath, methodName: methodName, lineNumber);
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
        protected ValueTask ZeroAsyncOption<T>(Func<T, ValueTask> continuation, T state,
            CancellationToken asyncToken, TaskCreationOptions options, TaskScheduler scheduler = null,
            bool unwrap = true, [CallerFilePath] string filePath = null, [CallerMemberName] string methodName = null,
            [CallerLineNumber] int lineNumber = default)
        {
            return ZeroAsyncOption(continuation, state, options, scheduler, unwrap, filePath, methodName, lineNumber);
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
            /// <param name="innerException">The inner Exception</param>
            /// <param name="methodName">The calling method name</param>
            /// <param name="lineNumber">The calling method line number</param>
            /// <returns>The new exception</returns>
            public static ZeroException ErrorReport(object state, string message = null, Exception innerException = null, [CallerMemberName] string filePath = null, [CallerMemberName] string methodName = null, [CallerLineNumber] int lineNumber = default )
            {
#if DEBUG
                if (state == null)
                    throw new NullReferenceException($"{nameof(state)}");
#endif
                
                var nanite = state as IoNanoprobe;
                var description = nanite?.Description ?? $"{state}";
                
                return new ZeroException($"{Path.GetFileName(filePath)}:{methodName}() {lineNumber} - [{description}]: {message}", innerException);
            }
        }
        
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="other"></param>
        /// <returns>True if equal, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(IIoNanite other)
        {
            return NpId == other.NpId;
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