using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
        private ConcurrentDictionary<Func<IIoZeroable, Task>, object> _subscribers = new ConcurrentDictionary<Func<IIoZeroable, Task>, object>();

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
        public Task Zero(IIoZeroable from)
        {
            if (_zeroed > 0)
                return Task.CompletedTask;

            if (from != this)
                ZeroedFrom = from;

            Dispose();

            return Task.CompletedTask;
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
        public Func<IIoZeroable, Task> ZeroEvent(Func<IIoZeroable, Task> sub)
        {
            if (!_subscribers?.TryAdd(sub, null)??false)//TODO race condition?
            {
                if(_subscribers != null)
                    LogManager.GetCurrentClassLogger().Warn($"Event already subscribed: Method = {sub.Method}, Target = {sub.Target}");
            }

            return sub;
        }

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        public Task Unsubscribe(Func<IIoZeroable, Task> sub)
        {
            if (sub == null)
                return null;

            if (!_subscribers?.TryRemove(sub, out _)??false)
            {
                LogManager.GetCurrentClassLogger().Warn($"Cannot unsubscribe, event not found: Method = {sub.Method}, Target = {sub.Target}");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Cascade zero the <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">Enforces mutual zero</param>
        public T ZeroOnCascade<T>(T target, bool twoWay = false)
        where T:IIoZeroable
        {
            var sub = ZeroEvent((sender) => target.Zero(this));

            if (twoWay) //zero
            {
                target.ZeroEvent((s) => Zero(target));
            }
            else //Release
            {
                target.ZeroEvent(z=>Unsubscribe(sub));
            }
                
            return target;
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</param>
        protected virtual async void Zero(bool disposing)
        {
            if( Interlocked.CompareExchange(ref _zeroed, 1, 0) > 0 )
                return;

            //lock (this)
            //{
            //    if (_zeroed > 0)
            //        return;

            //    Interlocked.Increment(ref _zeroed);
            //    //PrintPathToZero();
            //}
            
            AsyncTasks.Cancel();

            //emit zero event
            foreach (var handler in _subscribers.Keys)
            {
                try
                {
                    await handler(this).ConfigureAwait(false);
                }
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
