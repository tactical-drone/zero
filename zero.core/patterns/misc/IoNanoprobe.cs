using System;
using System.Collections.Concurrent;
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
        public IoNanoprobe()
        {
            
        }

        /// <summary>
        /// Constructs a nano probe
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="concurrencyLevel">Maximum blockers allowed. Consumption: 128 bits per tick.</param>
        public IoNanoprobe(string description, int concurrencyLevel = 1)
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
        /// Are we disposed?
        /// </summary>
        private volatile int _zeroed;

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
        /// ZeroAsync pattern
        /// </summary>
        public void Dispose()
        {
            ZeroedFrom = new IoNanoprobe("Dispose");
#pragma warning disable 4014
            ZeroAsync(true);
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
            ZeroedFrom ??= !@from.Equals(this) ? @from : Sentinel;
            
            if (_zeroed > 0)
                return;
            
            //ZeroedFrom ??= !@from.Equals(this) ? @from : new IoNanoprobe("sentinel");
            
            //ZeroedFrom = !@from.Equals(this) ? @from : new IoNanoprobe("self");

            await ZeroAsync(true).FastPath().ConfigureAwait(false);
            
            GC.SuppressFinalize(this);
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
                if (cur == this as IIoNanite)
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
        /// <returns>The handler</returns>
        public IoZeroSub ZeroSubscribe(Func<IIoNanite, ValueTask> sub)
        {
            IoZeroSub newSub;
            _zeroSubs.Add(newSub = new IoZeroSub
            {
                Action = sub,
                Schedule = true
            });

            //regular cleanups
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
        public ValueTask Unsubscribe(IoZeroSub sub)
        {
            sub.Schedule = false;
            sub.Action = null;
            return ValueTask.CompletedTask;
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

            ZeroSubscribe(async from => await target.ZeroAsync(from).FastPath().ConfigureAwait(false));

            if (twoWay) //zero
                target.ZeroSubscribe(async from => await ZeroAsync(target).FastPath().ConfigureAwait(false));
            
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

            // No races allowed between shutting down and starting up
            //await ZeroAtomicAsync(async (s, u, isDisposing) =>
            //{
            //var @this = (IoNanoprobe) s;
            var @this = (IoNanoprobe)this;
            @this.CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    if(!@this.AsyncTasks.IsCancellationRequested)
                        @this.AsyncTasks.Cancel();
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Cancel async tasks failed for {@this.Description}");
                }

                //emit zero event
                foreach(var zeroSub in _zeroSubs)
                {
                    try
                    {
                        if (!zeroSub.Schedule)
                            continue;

                        if (zeroSub.Action != null)
                        {
                            var zeroAction = zeroSub.Action;
                            var u = Unsubscribe(zeroSub).FastPath().ConfigureAwait(false);
                            await zeroAction(this).FastPath().ConfigureAwait(false);
                        }
                    }
                    //catch (NullReferenceException e)
                    //{
                    //    _logger.Trace(e, @this.Description);
                    //}
                    catch (Exception e)
                    {

                        try
                        {
                            _logger.Fatal(e, $"zero sub {((IIoNanite) zeroSub?.Action?.Target)?.Description} on {@this.Description} returned with errors!");
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }

                @this.CascadeTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - @this.CascadeTime;
                @this.TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                if (disposing)
                {
                    //Dispose managed
                    try
                    {
                        await @this.ZeroManagedAsync().FastPath().ConfigureAwait(false);
                    }
                    //catch (NullReferenceException) { }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"[{@this}] {nameof(ZeroManagedAsync)} returned with errors!");
                    }
                }

                //Dispose unmanaged
                try
                {
                    @this.AsyncTasks.Dispose();

                    @this.ZeroUnmanaged();

                    @this.AsyncTasks = null;
                    @this.ZeroedFrom = null;
                    @this._zeroSubs = null;
                }
                catch (NullReferenceException)
                {
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"ZeroAsync [Un]managed errors: {@this.Description}");
                }

                @this.TearDownTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - @this.TearDownTime;
                //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
                //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");

                try
                {
                    if (@this.Uptime.ElapsedSec() > 10 && @this.CascadeTime > @this.TearDownTime * 2 && @this.TearDownTime > 0)
                        _logger.Fatal(
                            $"{@this.GetType().Name}:Z/{@this.Description}> SLOW TEARDOWN!, c = {@this.CascadeTime:0.0}ms, t = {@this.TearDownTime:0.0}ms, count = {_zeroSubs.Count}");
                }
                catch
                {
                    // ignored
                }

            //    return true;
            //}, disposing: disposing, force: false).ConfigureAwait(false);
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
        public virtual async ValueTask<bool> ZeroAtomicAsync(Func<IIoNanite, object, bool, ValueTask<bool>> ownershipAction,
            object userData = null,
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