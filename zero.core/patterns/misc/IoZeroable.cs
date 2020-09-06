using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Zero teardown
    /// </summary>
    public class IoZeroable:IDisposable, IIoZeroable
    {
        /// <summary>
        /// Destructor
        /// </summary>
        ~IoZeroable()
        {
            Zero(false);
        }

        /// <summary>
        /// Description
        /// </summary>
        public virtual string Description => $"{GetType().Name}";

        /// <summary>
        /// Who zeroed this object
        /// </summary>
        public IIoZeroable ZeroedFrom { get; protected set; }

        /// <summary>
        /// Cancellation token source
        /// </summary>
        private CancellationTokenSource _asyncTasks = new CancellationTokenSource();

        /// <summary>
        /// Used by superclass to manage all async calls
        /// </summary>
        protected CancellationTokenSource AsyncTasks => _asyncTasks;

        /// <summary>
        /// Are we disposed?
        /// </summary>
        private volatile int _zeroed;

        /// <summary>
        /// All subscriptions
        /// </summary>
        private ConcurrentDictionary<Func<IIoZeroable, ValueTask<bool>>, object> _subscribers = new ConcurrentDictionary<Func<IIoZeroable, ValueTask<bool>>, object>();

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
#pragma warning disable 1998
        public async ValueTask<bool> Zero(IIoZeroable from)
#pragma warning restore 1998
        {
            if (_zeroed > 0)
                return false;

            if (from != this)
                ZeroedFrom = from;

            Dispose();

            return true;
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

            LogManager.GetCurrentClassLogger().Debug($"[{GetType().Name}]{Description}: ZEROED from: {(!string.IsNullOrEmpty(builder.ToString()) ? builder.ToString() : "this")}");
        }

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        public bool Zeroed()
        {
            return _zeroed > 0;
        }

        /// <summary>
        /// Subscribe to disposed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        public Func<IIoZeroable, ValueTask<bool>> ZeroEvent(Func<IIoZeroable, ValueTask<bool>> sub)
        {
            try
            {
                if (!_subscribers.TryAdd(sub, null))
                {
                    LogManager.GetCurrentClassLogger().Warn($"Event already subscribed: Method = {sub.Method}, Target = {sub.Target}");
                }
            }
            catch (NullReferenceException) { }

            return sub;
        }

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
#pragma warning disable 1998
        public async ValueTask<bool> Unsubscribe(Func<IIoZeroable, ValueTask<bool>> sub)
#pragma warning restore 1998
        {
            if (sub == null)
                return false;

            try
            {
                if (_subscribers.Count != 0 && !_subscribers.TryRemove(sub, out _))
                {
                    LogManager.GetCurrentClassLogger().Debug($"Cannot unsubscribe from {Description}, event not found: Method = {sub.Method}, Target = {sub.Target}");
                }
            }
            catch (NullReferenceException) { }

            return true;
        }

        /// <summary>
        /// Cascade zero the <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">Enforces mutual zero</param>
        public T ZeroOnCascade<T>(T target, bool twoWay = false)
        where T:IIoZeroable
        {
            if (twoWay)//zero
            {
                Func<IIoZeroable, ValueTask<bool>> sourceZeroHandler = null;
                Func<IIoZeroable, ValueTask<bool>> targetZeroHandler = null;

                // ReSharper disable once AccessToModifiedClosure
                sourceZeroHandler = ZeroEvent(s => s == (IIoZeroable) target ? Unsubscribe(sourceZeroHandler) : target.Zero(this));

                // ReSharper disable once AccessToModifiedClosure
                targetZeroHandler = target.ZeroEvent(s => s == this ? target.Unsubscribe(targetZeroHandler) : Zero(target));

                ////Target zero logic
                //target.ZeroEvent(s => s == this ? Unsubscribe(ZeroTarget) : ZeroSource(s));

                ////Source zero logic
                //ZeroEvent(s => s == (IIoZeroable) target ? target.Unsubscribe(ZeroSource) : ZeroTarget(s));
            }
            else //Release source if target goes
            {
                var sub = ZeroEvent(target.Zero);

#pragma warning disable 1998
                target.ZeroEvent(async s => s != this && Unsubscribe(sub).Result);
#pragma warning restore 1998
            }

            return target;
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</param>
        protected virtual void Zero(bool disposing)
        {
            if( Interlocked.CompareExchange(ref _zeroed, 1, 0) > 0 )
                return;

            AsyncTasks.Cancel();

            //emit zero event
            foreach (var handler in _subscribers.Keys)
            {
                try
                {
                    handler(this);
                }
                catch (NullReferenceException) { }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Fatal(e, $"[{ToString()}] returned with errors!");
                }
            }

            //clear out subscribers
            _subscribers.Clear();
            
            //Dispose managed
            try
            {
                ZeroManaged();
            }
            //catch (NullReferenceException) { }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e, $"[{ToString()}] {nameof(ZeroManaged)} returned with errors!");
            }

            //Dispose unmanaged
            if (disposing)
            {
                try
                {
                    _asyncTasks.Dispose();
                    
                    ZeroUnmanaged();

                    _asyncTasks = null;
                    ZeroedFrom = null;
                    _subscribers = null;
                }
                catch (NullReferenceException) { }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Error(e,$"[{ToString()}] {nameof(ZeroUnmanaged)} returned with errors!");
                }
            }
        }

        /// <summary>
        /// Manages unmanaged objects
        /// </summary>
        protected virtual void ZeroUnmanaged() { }

        /// <summary>
        /// Manages managed objects
        /// </summary>
        protected virtual void ZeroManaged() { }
    }
}
