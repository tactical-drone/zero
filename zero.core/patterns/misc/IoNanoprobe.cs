using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.semaphore;
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

        public IoNanoprobe(string description = null)
        {
            Description = description ?? GetType().Name;

            _zeroSubs = new ConcurrentStack<IoZeroSub>();
            _zeroed = 0;
            ZeroedFrom = default;
            TearDownTime = default;
            CascadeTime = default;
            Uptime = DateTimeOffset.UtcNow.UtcTicks;
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTasks = new CancellationTokenSource();
            
            var enableFairQ = false;
            var enableDeadlockDetection = true;
#if RELEASE
            enableDeadlockDetection = false;
#endif

            _nanoMutex = new IoZeroSemaphore(nameof(_nanoMutex), initialCount: 1, maxCount: 100, enableDeadlockDetection: enableDeadlockDetection, enableFairQ:enableFairQ);
            _nanoMutex.ZeroRef(ref _nanoMutex, AsyncTasks.Token);
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~IoNanoprobe()
        {
            ZeroAsync(false).ConfigureAwait(false); //.GetAwaiter().GetResult();
        }

        /// <summary>
        /// 
        /// </summary>
        private static ILogger _logger;

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
        public long Uptime;

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
        private ConcurrentStack<IoZeroSub> _zeroSubs;

        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        public void Dispose()
        {
            ZeroAsync(true).ConfigureAwait(false); //.GetAwaiter().GetResult();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// ZeroAsync
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask ZeroAsync(IIoNanite from)
        {
#if DEBUG
            if (from == null)
                throw new NullReferenceException(nameof(from));
#endif

            if (_zeroed > 0)
                return;

            if (!from.Equals(this))
                ZeroedFrom = from;

            await ZeroAsync(true).ConfigureAwait(false);
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
            return _zeroed > 0;
        }

        /// <summary>
        /// Subscribe to zero event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        public IoZeroSub ZeroEvent(Func<IIoNanite, Task> sub)
        {
            IoZeroSub newSub;
            _zeroSubs.Push(newSub = new IoZeroSub
            {
                Action = sub,
                Schedule = true
            });

            return newSub;
        }

        /// <summary>
        /// Unsubscribe to a zero event
        /// </summary>
        /// <param name="sub">The original subscription</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unsubscribe(IoZeroSub sub)
        {
            sub.Schedule = false;
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

            if (twoWay) //zero
            {
                IoZeroSub sourceZeroHandler = default;
                IoZeroSub targetZeroHandler = default;

                sourceZeroHandler = ZeroEvent(async s =>
                {
                    if (s.Equals(target))
                        Unsubscribe(sourceZeroHandler);
                    else
                        await target.ZeroAsync(s).ConfigureAwait(false);
                });

                // ReSharper disable once AccessToModifiedClosure
                targetZeroHandler = target.ZeroEvent(async s =>
                {
                    if (s.Equals(this))
                        target.Unsubscribe(targetZeroHandler);
                    else
                        await ZeroAsync(s).ConfigureAwait(false);
                });
            }
            else
            {
                var sub = ZeroEvent(async from => await target.ZeroAsync(from).ConfigureAwait(false));

                target.ZeroEvent(s =>
                {
                    if (!s.Equals(this))
                        Unsubscribe(sub);
                    return Task.CompletedTask;
                });
            }

            return (target, true);
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</
        [MethodImpl(MethodImplOptions.AggressiveInlining)] 
        private async ValueTask ZeroAsync(bool disposing)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) > 0)
                return;
            
            // No races allowed between shutting down and starting up
            await ZeroAtomicAsync(async (s, u, isDisposing) =>
            {
                var @this = (IoNanoprobe) s;
                @this.CascadeTime = DateTime.Now.Ticks;
                try
                {
                    @this.AsyncTasks.Cancel();
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Cancel async tasks failed for {@this.Description}");
                }

                @this.TearDownTime = DateTime.Now.Ticks;

                var subs = new IoZeroSub[10];
                var popped = 0;
                //emit zero event
                while ((popped = @this._zeroSubs.TryPopRange(subs)) > 0)
                {
                    for (var i = 0; i < popped; i++)
                    {
                        var zeroSub = subs[i];
                        try
                        {
                            if (!zeroSub.Schedule)
                                continue;
                            await zeroSub.Action(ZeroedFrom ?? this).ConfigureAwait(false);
                        }
                        catch (NullReferenceException e)
                        {
                            _logger.Trace(e, @this.Description);
                        }
                        catch (Exception e)
                        {
                            _logger.Fatal(e,
                                $"zero sub {((IIoNanite) zeroSub.Action.Target)?.Description} on {@this.Description} returned with errors!");
                        }
                    }
                }

                @this.CascadeTime = DateTimeOffset.Now.Ticks;

                //Dispose managed
                try
                {
                    await @this.ZeroManagedAsync().ConfigureAwait(false);
                }
                //catch (NullReferenceException) { }
                catch (Exception e)
                {
                    _logger.Error(e, $"[{@this.ToString()}] {nameof(@this.ZeroManagedAsync)} returned with errors!");
                }

                //Dispose unmanaged
                if (isDisposing)
                {
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
                }

                @this.TearDownTime = DateTime.Now.Ticks;
                //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
                //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");
                if (@this.Uptime.TickSec() > 10 && @this.TearDownTime.TickMs() > @this.CascadeTime.TickMs() + 200)
                    _logger.Fatal(
                        $"{@this.GetType().Name}:Z/{@this.Description}> SLOW TEARDOWN!, t = {@this.TearDownTime.TickMs() / 1000.0:0.000}, c = {@this.CascadeTime.TickMs() / 1000.0:0.000}");
                _logger = null;

                return true;
            }, disposing: disposing, force: true).ZeroBoostAsync().ConfigureAwait(false);
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
            
        }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ValueTask ZeroManagedAsync()
        {
            return ValueTask.CompletedTask;
        }

        ///  <summary>
        ///  Ensures that a ownership transfer action is synchronized
        ///  </summary>
        ///  <param name="ownershipAction">The ownership transfer callback</param>
        ///  <param name="userData"></param>
        ///  <param name="disposing">If disposing</param>
        ///  <param name="force">Forces the action regardless of zero state</param>
        ///  <returns>true if ownership was passed, false otherwise</returns>s
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<bool> ZeroAtomicAsync(Func<IIoNanite, object, bool, ValueTask<bool>> ownershipAction,
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
                            if (await _nanoMutex.WaitAsync().ZeroBoostAsync().ConfigureAwait(false))
                            {
                                return (_zeroed == 0) &&
                                       await ownershipAction(this, userData, disposing).ZeroBoostAsync().ConfigureAwait(false);
                            }
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e,
                                $"{Description}: Unable to ensure action {ownershipAction}, target = {ownershipAction.Target}");
                            return false;
                        }
                        finally
                        {
                            _nanoMutex.Release();
                        }
                    }
                    else
                    {
                        return await ownershipAction(this, userData, disposing).ZeroBoostAsync().ConfigureAwait(false);
                    }
                }
                catch
                {
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
            return Description;
        }
    }
}