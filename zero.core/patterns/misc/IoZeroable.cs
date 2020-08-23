using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;

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
        public Task Zero()
        {
            Dispose();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Are we disposed?
        /// </summary>
        private volatile bool _zeroed;

        /// <summary>
        /// All subscriptions
        /// </summary>
        private readonly ConcurrentDictionary<Func<IIoZeroable, Task>, object> _subscribers = new ConcurrentDictionary<Func<IIoZeroable, Task>, object>();

        /// <summary>
        /// Indicate zero status
        /// </summary>
        /// <returns>True if zeroed out, false otherwise</returns>
        public bool Zeroed()
        {
            return _zeroed;
        }

        /// <summary>
        /// Subscribe to disposed event
        /// </summary>
        /// <param name="sub">The handler</param>
        /// <returns>The handler</returns>
        public Func<IIoZeroable, Task> ZeroEvent(Func<IIoZeroable, Task> sub)
        {
            if (!_subscribers.TryAdd(sub, null))
            {
                LogManager.GetCurrentClassLogger().Warn($"Event already subscribed: Method = {sub.Method}, Target = {sub.Target}");
            }

            return sub;
        }

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        public Func<IIoZeroable, Task> Unsubscribe(Func<IIoZeroable, Task> sub)
        {
            if (!_subscribers.TryRemove(sub, out _))
            {
                LogManager.GetCurrentClassLogger().Warn($"Cannot unsubscribe, event not found: Method = {sub.Method}, Target = {sub.Target}");
            }
            return sub;
        }

        /// <summary>
        /// Cascade zero the <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">If the zeroing out goes both ways</param>
        public T ZeroOnCascade<T>(T target, bool twoWay = false)
        where T:IIoZeroable
        {
            ZeroEvent((sender) => target.Zero());
            if (twoWay)
            {
                target.ZeroEvent((s) => Zero());
            }
                
            return target;
        }

        /// <summary>
        /// Our dispose implementation
        /// </summary>
        /// <param name="disposing">Whether we are disposing unmanaged objects</param>
        protected virtual void Zero(bool disposing)
        {
            lock (this)
            {
                if (_zeroed)
                    return;

                _zeroed = true;
            }

            //emit zero event
            foreach (var handler in _subscribers.Keys)
            {
                try
                {
                    handler(this).Wait();
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Fatal(e, $"[{ToString()}] returned with errors!");
                }
            }
            
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
                    ZeroUnmanaged();
                }
                catch (Exception e)
                {
                    LogManager.GetCurrentClassLogger().Error(e,$"[{ToString()}] {nameof(ZeroUnmanaged)} returned with errors!");
                }
            }

            //clear out subscribers
            _subscribers.Clear();
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
