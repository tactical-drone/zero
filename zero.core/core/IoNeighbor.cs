using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.data.contracts;
using zero.core.data.providers.cassandra;
using zero.core.data.providers.cassandra.keyspaces.tangle;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled;
using Logger = NLog.Logger;

namespace zero.core.core
{
    /// <inheritdoc />
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class IoNeighbor<TJob> : IoProducerConsumer<TJob>
    where TJob : IIoWorker
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="type">A description of the neighbor</param>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocMessage">The callback that allocates new message buffer space</param>
        public IoNeighbor(string type, IoNetClient<TJob> ioNetClient, Func<object, IoConsumable<TJob>> mallocMessage)
            : base($"{type} `{ioNetClient.Description}'", ioNetClient, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();

            Spinners.Token.Register(() => PrimaryProducer?.Close());
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        private bool _closed = false;

        /// <summary>
        /// Called when this neighbor is closed
        /// </summary>
        public event EventHandler Closed;

        /// <summary>
        /// Close this neighbor
        /// </summary>
        public void Close()
        {
            lock (this)
            {
                if (_closed) return;
                _closed = true;
            }
            
            _logger.Info($"Closing neighbor `{PrimaryProducerDescription}'");

            OnClosed();
            
            Spinners.Cancel();            
        }

        /// <summary>
        /// Emits the closed event
        /// </summary>
        public virtual void OnClosed()
        {
            Closed?.Invoke(this, EventArgs.Empty);
        }               
    }
}
