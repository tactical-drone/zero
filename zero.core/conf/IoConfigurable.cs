using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reflection;
using zero.core.patterns.bushes;
using NLog;
using Observables.Specialized.Extensions;

namespace zero.core.conf
{
    /// <summary>
    /// Configuration storage, retrieval and mutation
    /// </summary>
    public class IoConfigurable
    {
        /// <summary>
        /// Constructor that gathers all parameters from instances by scanning for the <see cref="IoParameter"/> attribute
        /// </summary>
        public IoConfigurable()
        {
            //Initialize the parameter dictonary
            GetType().GetFields().ToList().Where(p => p.IsDefined(typeof(IoParameter))).ToList().ForEach(
                p =>
                {
                        LocalConfigBus.GetOrAdd(p.Name, c => p.GetValue(this));
                });

            //Subscribe to changes
            _localConfigBusSubscription = LocalConfigBus.Subscribe(o =>
            {
                //If an item was updated
                if ( o.EventType == DictionaryUpdatedEventType.itemUpdated)
                {
                    GetType().GetFields().ToList().Where(p => p.Name == o.Key).ToList().ForEach(p =>
                    {
                        //Set it's instance value
                        p.SetValue(this, o.Value);

                        //Emit event
                        SettingChangedEvent?.Invoke(this, new KeyValuePair<string, object>(o.Key, o.Value));
                    });
                }
            });

            lock (GlobalConfigBus)
            {
                //Merge the two streams
                GlobalConfigBus.Merge(LocalConfigBus);
            }
        }

        /// <summary>
        /// Emitted when a setting has changed
        /// </summary>
        public static EventHandler<KeyValuePair<string, object>> SettingChangedEvent;

        /// <summary>
        /// A dictionary containing all settings over all instances
        /// </summary>
        public static ObservableDictionary<string, object> GlobalConfigBus = new ObservableDictionary<string, object>();
        //TODO get rid of this lib it leaks memory

        /// <summary>
        /// A dictionary containing all instance settings
        /// </summary>
        protected ObservableDictionary<string, object> LocalConfigBus = new ObservableDictionary<string, object>();


        /// <summary>
        /// A handle to the subscription to the local config bus 
        /// </summary>
        private IDisposable _localConfigBusSubscription;

        /// <summary>
        /// Signals upstream systems that this instance has been reconfigured
        /// </summary>
        public bool Reconfigure;
    }
}
