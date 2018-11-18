﻿using System;
using System.Threading.Tasks;
using NLog;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.misc;

namespace zero.core.core
{
    /// <inheritdoc />
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class IoNeighbor : IoProducerConsumer<IoMessage<IoNetClient>, IoNetClient>
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocMessage">The callback that allocates new message buffer space</param>
        public IoNeighbor(IoNetClient ioNetClient, Func<IoMessage<IoNetClient>> mallocMessage)
            : base($"neighbor {ioNetClient.AddressString}", ioNetClient, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();

            Spinners.Token.Register(() => WorkSource?.Close());
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Called when this neighbor is closed
        /// </summary>
        public event EventHandler Closed;
       

        /// <summary>
        /// Close this neighbor
        /// </summary>
        public void Close()
        {
            _logger.Info($"Closing neighbor `{Description}'");

            Spinners.Cancel();            

            OnClosed();
        }

        /// <summary>
        /// Emits the closed event
        /// </summary>
        protected virtual void OnClosed()
        {
            Closed?.Invoke(this, EventArgs.Empty);
        }
    }
}
