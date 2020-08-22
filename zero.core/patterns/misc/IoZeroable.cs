using System;
using System.Collections.Concurrent;
using System.Linq;
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
        public void Zero()
        {
            Dispose();
        }

        /// <summary>
        /// Are we disposed?
        /// </summary>
        private volatile bool _zeroed;

        /// <summary>
        /// Emits the disposed event
        /// </summary>
        private event EventHandler _zeroEvent;

        /// <summary>
        /// All subscriptions
        /// </summary>
        private readonly ConcurrentBag<EventHandler> _subscribers = new ConcurrentBag<EventHandler>();

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
        public EventHandler ZeroEvent(EventHandler sub)
        {
            _subscribers.Add(sub);
            _zeroEvent += sub;
            return sub;
        }

        /// <summary>
        /// Unsubscribe
        /// </summary>
        /// <param name="sub">The original subscription</param>
        public void Unsubscribe(EventHandler sub)
        {
            _zeroEvent -= sub;
        }

        /// <summary>
        /// Cascade zero the <see cref="target"/>
        /// </summary>
        /// <param name="target">The object to be zeroed out</param>
        /// <param name="twoWay">If the zeroing out goes both ways</param>
        public T ZeroOnCascade<T>(T target, bool twoWay = false)
        where T:IIoZeroable
        {
            ZeroEvent((sender, args) => target.Zero());
            if (twoWay)
            {
                target.ZeroEvent((sender, args) => Zero());
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

            //zeroing event
            try
            {
                _zeroEvent?.Invoke(this, EventArgs.Empty);
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Fatal(e, $"[{ToString()}] {nameof(_zeroEvent)} returned with errors!");
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
            foreach (var eventHandler in _subscribers)
            {
                _zeroEvent -= eventHandler;
            }

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
