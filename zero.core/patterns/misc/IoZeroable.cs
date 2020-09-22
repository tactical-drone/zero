using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.misc;
using zero.core.patterns.semaphore;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// ZeroAsync teardown
    /// </summary>
    public struct IoZeroable<TMutex> : IIoZeroable, IDisposable where TMutex : struct, IIoMutex
    {
        /// <summary>
        /// static constructor
        /// </summary>
        static IoZeroable()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        public IoZeroable(CancellationTokenSource asyncTokenProxy):this()
        {
            _zeroSubs = new ConcurrentStack<IoZeroSub>();
            _zeroMutex = new TMutex();
            _zeroed = 0;
            ZeroedFrom = default;
            TearDownTime = default;
            CascadeTime = default;
            Uptime = default;
            _zId = Interlocked.Increment(ref _uidSeed);
            AsyncTokenProxy = asyncTokenProxy;
        }

        /// <summary>
        /// Destructor
        /// </summary>
        // ~IoZeroable()
        // {
        //     ZeroAsync(false).ConfigureAwait(false);//.GetAwaiter().GetResult();
        // }
        
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
        public string Description => $"{nameof(IoZeroable<TMutex>)}";
        
        //private IoHeap<TMutex> _mutHeap;
        
        /// <summary>
        /// Sync root
        /// </summary>
        private readonly TMutex _zeroMutex;

        /// <summary>
        /// Who zeroed this object
        /// </summary>
        public IIoZeroable ZeroedFrom { get; private set; }

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
            ZeroAsync(true).ConfigureAwait(false);//.GetAwaiter().GetResult();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// ZeroAsync
        /// </summary>
        public async ValueTask ZeroAsync(IIoZeroable from)
        {
            if (_zeroed > 0)
                return;

            if (from.GetHashCode() != GetHashCode())
                ZeroedFrom = (IIoZeroable)from;
            
            await ZeroAsync(true).ConfigureAwait(false);
            GC.SuppressFinalize(obj: this);
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
                if (cur == this as IIoZeroable)
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
        public IoZeroSub ZeroEvent(Func<IIoZeroable, Task> sub)
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
        public (T target, bool success) ZeroOnCascade<T>(T target, bool twoWay = false) where T : IIoZeroable
        {
             if(_zeroed > 0)
                return (default, false);

             if (twoWay)//zero
             {
                 IoZeroSub sourceZeroHandler = default;
                 IoZeroSub targetZeroHandler = default;
                 
                 
                 var handler = sourceZeroHandler;
                 IIoZeroable tmpThis = this;
                 sourceZeroHandler = tmpThis.ZeroEvent(async s =>
                 {
                     if (s.Equals(target))
                         tmpThis.Unsubscribe(handler);
                     else
                         await target.ZeroAsync(tmpThis).ConfigureAwait(false);

                 });

                 // ReSharper disable once AccessToModifiedClosure
                 IoZeroable<TMutex> @this = this;
                 targetZeroHandler = target.ZeroEvent(async s =>
                 {
                     if (s.Equals(@this))
                         target.Unsubscribe(targetZeroHandler);
                     else
                         await @this.ZeroAsync(target).ConfigureAwait(false);
                 });
             }
             else //Release source if target goes
             {
                 var sub = ZeroEvent(async from => await target.ZeroAsync(@from).ConfigureAwait(false));

                 IoZeroable<TMutex> @this = this;
                 target.ZeroEvent(s =>
                 {
                     if (!s.Equals(@this))
                         @this.Unsubscribe(sub);
                     return Task.CompletedTask;
                 });
             }

             return (target, true);
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</param>
        private async ValueTask ZeroAsync(bool disposing)
        {
            // Only once
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) > 0)
                return;

            // No races allowed between shutting down and starting up
            var @this = this;
            CancellationTokenSource asyncTasks = AsyncTokenProxy;
            await @this.ZeroEnsureAsync(async (s) =>
            {
                @this.CascadeTime = DateTime.Now.Ticks;
                try
                {
                    //TODO
                    asyncTasks.Cancel();
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
                            await zeroSub.Action(@this).ConfigureAwait(false);
                        }
                        catch (NullReferenceException e)
                        {
                            _logger.Trace(e,@this.Description);
                        }
                        catch (Exception e)
                        {
                            _logger.Fatal(e, $"zero sub {((IIoZeroable)zeroSub.Action.Target)?.Description} on {@this.Description} returned with errors!");
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
                if (disposing)
                {
                    try
                    {
                        asyncTasks.Dispose();

                        @this.ZeroUnmanaged();

                        //_logger = null;
                        asyncTasks = null;
                        @this.ZeroedFrom = null;
                        @this._zeroSubs = null; 
                    }
                    catch (NullReferenceException) { }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"ZeroAsync [Un]managed errors: {@this.Description}");
                    }
                }

                @this.TearDownTime = DateTime.Now.Ticks;
                //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
                //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");
                if (@this.Uptime.TickSec() > 10 && @this.TearDownTime.TickSec() > @this.CascadeTime.TickSec() + 200)
                    _logger.Fatal($"{@this.GetType().Name}:Z/{@this.Description}> SLOW TEARDOWN!, t = {@this.TearDownTime.TickSec()/1000.0:0.000}, c = {@this.CascadeTime.TickSec()/1000.0:0.000}");
                _logger = null;

                return true;
            }, true).ConfigureAwait(false);
        }

        
        public CancellationTokenSource AsyncTokenProxy { get; }

        /// <summary>
        /// Manages unmanaged objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroUnmanaged() { }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ZeroManagedAsync() {return ValueTask.CompletedTask;}

        /// <summary>
        /// Ensures that a ownership transfer action is synchronized
        /// </summary>
        /// <param name="ownershipAction">The ownership transfer callback</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true if ownership was passed, false otherwise</returns>
        ///public async ValueTask<bool> ZeroEnsureAsync(Func<IIoZeroable<TMutex>, Task<bool>>  ownershipAction, bool force = false)
        public async ValueTask<bool> ZeroEnsureAsync(Func<IIoZeroable, Task<bool>> ownershipAction, bool force = false)
        {
            try
            {
                //Prevent strange things from happening
                if (_zeroed > 0 && !force)
                    return false;

                ValueTask<bool> syncRootTask = default;
                try
                {
                    syncRootTask = _zeroMutex.WaitAsync();
                    await syncRootTask.OverBoostAsync().ConfigureAwait(false);

                    return (_zeroed == 0 || force) && syncRootTask.Result &&
                           await ownershipAction(this).ConfigureAwait(false);
                }
                catch
                {
                    return false;
                }
                finally
                {
                    _zeroMutex.Set();
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
            finally
            {
                //if (mutex != default)
                {
                    _zeroMutex.Set();
                  //  await _mutHeap.ReturnAsync(mutex).ConfigureAwait(false);
                }
            }
        }

        public bool Equals(IIoZeroable other)
        {
            return NpId == other.NpId;
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
