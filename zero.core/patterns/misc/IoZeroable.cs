using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using NLog;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// ZeroAsync teardown
    /// </summary>
    public class IoZeroable : IDisposable, IIoZeroable
    {
        /// <summary>
        /// static constructor
        /// </summary>
        static IoZeroable()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        public IoZeroable(IIoMutex zeroMutex)
        {
            _syncRootAuto = zeroMutex?? ZeroOnCascade(new IoAutoMutex(true));
        }

        /// <summary>
        /// constructor
        /// </summary>
        public IoZeroable(bool zeroMutex = false)
        {
            _syncRootAuto = zeroMutex? (IIoMutex)this:ZeroOnCascade(new IoAutoMutex(true));
        }
        
        /// <summary>
        /// Destructor
        /// </summary>
        ~IoZeroable()
        {
            ZeroAsync(false).ConfigureAwait(false);//.GetAwaiter().GetResult();
        }

        /// <summary>
        /// 
        /// </summary>
        private static ILogger _logger;

        /// <summary>
        /// Description
        /// </summary>
        public virtual string Description => $"{GetType().Name}";

        /// <summary>
        /// A subscription
        /// </summary>
        public class ZeroSub
        {
            public Func<IIoZeroable, Task> Action;
            public volatile bool Schedule;
        }

        /// <summary>
        /// Sync root
        /// </summary>
        private readonly IIoMutex _syncRootAuto;

        /// <summary>
        /// Who zeroed this object
        /// </summary>
        public IIoZeroable ZeroedFrom { get; private set; }

        /// <summary>
        /// Measures how long teardown takes
        /// </summary>
        public Stopwatch TearDownTime { get; protected set; } = new Stopwatch();

        /// <summary>
        /// Measures how long cascading takes
        /// </summary>
        public Stopwatch CascadeTime { get; protected set; } = new Stopwatch();

        /// <summary>
        /// Uptime
        /// </summary>
        public Stopwatch Uptime { get; protected set; } = Stopwatch.StartNew();

        /// <summary>
        /// Used by superclass to manage all async calls
        /// </summary>
        public CancellationTokenSource AsyncTasks { get; private set; } = new CancellationTokenSource();

        /// <summary>
        /// Are we disposed?
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// All subscriptions
        /// </summary>
        private ConcurrentStack<ZeroSub> _zeroSubs = new ConcurrentStack<ZeroSub>();

        /// <summary>
        /// ZeroAsync pattern
        /// </summary>
        public void Dispose()
        {
            ZeroAsync(true).ConfigureAwait(false);//.GetAwaiter().GetResult();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// ZeroAsync
        /// </summary>
        public async Task ZeroAsync(IIoZeroable @from)
        {
            if (_zeroed > 0)
                return;

            if (from != this)
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
                if (cur == this)
                    break;
                cur = cur.ZeroedFrom;
            }

            _logger.Debug($"[{GetType().Name}]{Description}: ZEROED from: {(!string.IsNullOrEmpty(builder.ToString()) ? builder.ToString() : "this")}");
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
        public ZeroSub ZeroEvent(Func<IIoZeroable, Task> sub)
        {
            ZeroSub newSub = null;
            try
            {
                _zeroSubs.Push(newSub = new ZeroSub
                {
                    Action = sub,
                    Schedule = true
                });
            }
            catch (NullReferenceException) { }

            return newSub;
        }

        /// <summary>
        /// Unsubscribe to a zero event
        /// </summary>
        /// <param name="sub">The original subscription</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unsubscribe(ZeroSub sub)
        {
            if (sub == null)
                return;
            try
            {
                sub.Schedule = false;
            }
            catch (NullReferenceException) { }
        }

        /// <summary>
        /// Cascade zero events to <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">Enforces mutual zero</param>
        public T ZeroOnCascade<T>(T target, bool twoWay = false)
        where T : class, IIoZeroable
        {
            if (_zeroed > 0)
                return null;

            if (twoWay)//zero
            {
                ZeroSub sourceZeroHandler = null;
                ZeroSub targetZeroHandler = null;

                // ReSharper disable once AccessToModifiedClosure
                sourceZeroHandler = ZeroEvent(async s =>
                {
                    if (s == target)
                        Unsubscribe(sourceZeroHandler);
                    else
                        await target.ZeroAsync(this).ConfigureAwait(false);

                });

                // ReSharper disable once AccessToModifiedClosure
                targetZeroHandler = target.ZeroEvent(async s =>
                {
                    if (s == this)
                        target.Unsubscribe(targetZeroHandler);
                    else
                        await ZeroAsync(target).ConfigureAwait(false);
                });
            }
            else //Release source if target goes
            {
                var sub = ZeroEvent(async from => await target.ZeroAsync(@from).ConfigureAwait(false));

                target.ZeroEvent(s =>
                {
                    if (s != this)
                        Unsubscribe(sub);
                    return Task.CompletedTask;
                });
            }

            return target;
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</param>
        private async Task ZeroAsync(bool disposing)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) > 0)
                return;

            // No races allowed between shutting down and starting up
            await ZeroEnsureAsync(async () =>
            {
                CascadeTime.Restart();
                try
                {
                    //TODO
                    AsyncTasks.Cancel();
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Cancel async tasks failed for {Description}");
                }
                
                TearDownTime.Restart();

                var subs = new ZeroSub[10];
                var popped = 0;
                //emit zero event
                while ((popped = _zeroSubs.TryPopRange(subs)) > 0)
                {
                    for (var i = 0; i < popped; i++)
                    {
                        var zeroSub = subs[i];
                        try
                        {
                            if (!zeroSub.Schedule)
                                continue;
                            await zeroSub.Action(this).ConfigureAwait(false);
                        }
                        catch (NullReferenceException e)
                        {
                            _logger.Trace(e,Description);
                        }
                        catch (Exception e)
                        {
                            _logger.Fatal(e, $"zero sub {((IIoZeroable)zeroSub.Action.Target)?.Description} on {Description} returned with errors!");
                        }
                    }
                }

                CascadeTime.Stop();

                //Dispose managed
                try
                {
                    await ZeroManagedAsync().ConfigureAwait(false);
                }
                //catch (NullReferenceException) { }
                catch (Exception e)
                {
                    _logger.Error(e, $"[{ToString()}] {nameof(ZeroManagedAsync)} returned with errors!");
                }

                //Dispose unmanaged
                if (disposing)
                {
                    try
                    {
                        AsyncTasks.Dispose();

                        ZeroUnmanaged();

                        //_logger = null;
                        AsyncTasks = null;
                        ZeroedFrom = null;
                        _zeroSubs = null; 
                    }
                    catch (NullReferenceException) { }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"ZeroAsync [Un]managed errors: {Description}");
                    }
                }

                TearDownTime.Stop();
                //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
                //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");
                if (Uptime.Elapsed.TotalSeconds > 10 && TearDownTime.ElapsedMilliseconds > CascadeTime.ElapsedMilliseconds + 200)
                    _logger.Fatal($"{GetType().Name}:Z/{Description}> SLOW TEARDOWN!, t = {TearDownTime.ElapsedMilliseconds/1000.0:0.000}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.000}");
                _logger = null;

                return true;
            }, true).ConfigureAwait(false);
        }

        /// <summary>
        /// Manages unmanaged objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual void ZeroUnmanaged() { }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual Task ZeroManagedAsync() {return Task.CompletedTask;}

        /// <summary>
        /// Ensures that a ownership transfer action is synchronized
        /// </summary>
        /// <param name="ownershipAction">The ownership transfer callback</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true if ownership was passed, false otherwise</returns>
        public virtual async Task<bool> ZeroEnsureAsync(Func<Task<bool>> ownershipAction, bool force = false)
        {
            try
            {
                //Prevent strange things from happening
                if (_zeroed > 0 && !force) 
                    return false;

                try
                {
                    var syncRoot = _syncRootAuto.WaitAsync();
                    await syncRoot.OverBoostAsync().ConfigureAwait(false);

                    return (_zeroed == 0 || force) && syncRoot.Result && await ownershipAction().ConfigureAwait(false);
                }
                catch
                {
                    return false;
                }
                finally
                {
                    await _syncRootAuto.SetAsync().ConfigureAwait(false);
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e);
                return false;
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"Unable to ensure ownership in {Description}");
                return false;
            }
        }

        /// <summary>
        /// A description of this object
        /// </summary>
        /// <returns>A description</returns>
        public override string ToString()
        {
            return Description;
        }
    }
}
