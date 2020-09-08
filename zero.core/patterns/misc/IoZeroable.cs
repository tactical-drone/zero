﻿using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Zero teardown
    /// </summary>
    public class IoZeroable : IDisposable, IIoZeroable
    {
        public IoZeroable()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }
        /// <summary>
        /// Destructor
        /// </summary>
        ~IoZeroable()
        {
            Zero(false);
        }

        /// <summary>
        /// 
        /// </summary>
        private ILogger _logger;

        /// <summary>
        /// Description
        /// </summary>
        public virtual string Description => $"{GetType().Name}";

        /// <summary>
        /// A subscription
        /// </summary>
        public class ZeroSub
        {
            public Action<IIoZeroable> Action;
            public volatile bool Schedule;
        }

        /// <summary>
        /// Used to atomically transfer ownership
        /// </summary>
        private object _syncRoot = 0;

        /// <summary>
        /// Who zeroed this object
        /// </summary>
        public IIoZeroable ZeroedFrom { get; protected set; }

        /// <summary>
        /// Measures how long teardown takes
        /// </summary>
        public Stopwatch TeardownTime = new Stopwatch();

        /// <summary>
        /// Measures how long cascading takes
        /// </summary>
        public Stopwatch CascadeTime = new Stopwatch();

        /// <summary>
        /// Uptime
        /// </summary>
        public Stopwatch Uptime = Stopwatch.StartNew();

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
        /// Zero pattern
        /// </summary>
        public void Dispose()
        {
            Zero(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Zero
        /// </summary>
        public void Zero(IIoZeroable from)
        {
            if (_zeroed > 0)
                return;

            if (from != this)
                ZeroedFrom = from;

            Dispose();
        }

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
        /// Subscribe to disposed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        public ZeroSub ZeroEvent(Action<IIoZeroable> sub)
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
        /// Unsubscribe
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
        /// Cascade zero the <see cref="target"/>
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
                sourceZeroHandler = ZeroEvent(s =>
                {
                    if (s == target)
                        Unsubscribe(sourceZeroHandler);
                    else
                        target.Zero(this);
                });

                // ReSharper disable once AccessToModifiedClosure
                targetZeroHandler = target.ZeroEvent(s =>
                {
                    if (s == this)
                        target.Unsubscribe(targetZeroHandler);
                    else
                        Zero(target);
                });
            }
            else //Release source if target goes
            {
                var sub = ZeroEvent(target.Zero);

                target.ZeroEvent(s =>
                {
                    if (s != this)
                        Unsubscribe(sub);
                });
            }

            return target;
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</param>
        protected virtual void Zero(bool disposing)
        {
            if (_zeroed > 0 || Interlocked.CompareExchange(ref _zeroed, 1, 0) > 0)
                return;

            ZeroEnsure(() =>
            {

                CascadeTime.Restart();

                try
                {
                    AsyncTasks.Cancel(true);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Cancel async tasks failed for {Description}");
                }
                
                TeardownTime.Restart();

                //emit zero event
                while (_zeroSubs.TryPop(out var sub))
                {
                    try
                    {
                        if (!sub.Schedule)
                            continue;
                        sub.Action(this);
                    }
                    catch (NullReferenceException)
                    {
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e, $"zero sub {((IIoZeroable)sub.Action.Target)?.Description} on {Description} returned with errors!");
                    }
                }

                CascadeTime.Stop();

                //Dispose managed
                try
                {
                    ZeroManaged();
                }
                //catch (NullReferenceException) { }
                catch (Exception e)
                {
                    _logger.Error(e, $"[{ToString()}] {nameof(ZeroManaged)} returned with errors!");
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
                    catch (NullReferenceException)
                    {
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Zero [Un]managed errors: {Description}");
                    }
                }

                TeardownTime.Stop();
                //if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > 2000)
                //    _logger.Fatal($"{GetType().Name}:Z/{Description}> t = {TeardownTime.ElapsedMilliseconds/1000.0:0.0}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.0}");
                if (Uptime.Elapsed.TotalSeconds > 10 && TeardownTime.ElapsedMilliseconds > CascadeTime.ElapsedMilliseconds + 200)
                    _logger.Fatal($"{GetType().Name}:Z/{Description}> SLOW TEARDOWN!, t = {TeardownTime.ElapsedMilliseconds/1000.0:0.000}, c = {CascadeTime.ElapsedMilliseconds/1000.0:0.000}");
                _logger = null;

                return true;
            }, true);
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
        protected virtual void ZeroManaged() { }

        /// <summary>
        /// Ensures that a ownership transfer action is synchronized
        /// </summary>
        /// <param name="ownershipAction">The ownership transfer callback</param>
        /// <param name="force">Forces the action regardless of zero state</param>
        /// <returns>true if ownership was passed, false otherwise</returns>
        public virtual bool ZeroEnsure(Func<bool> ownershipAction, bool force = false)
        {
            try
            {
                //Prevent strange things from happening
                if (_zeroed > 0 && !force) return false;

                lock(_syncRoot)
                    return (_zeroed == 0 || force) && ownershipAction();
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

        public override string ToString()
        {
            return Description;
        }
    }
}
