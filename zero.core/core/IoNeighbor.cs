﻿using System;
using NLog;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using Logger = NLog.Logger;

namespace zero.core.core
{
    /// <inheritdoc />
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class IoNeighbor<TJob> : IoZero<TJob>
    where TJob : IIoJob
    {
        /// <summary>
        /// Construct
        /// </summary>        
        /// <param name="node">The node this neighbor is connected to</param>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocMessage">The callback that allocates new message buffer space</param>
        public IoNeighbor(IoNode<TJob> node, IoNetClient<TJob> ioNetClient, Func<object, IoLoad<TJob>> mallocMessage)
            : base($"{node.GetType().Name} neighbor: `{ioNetClient.Description}'", ioNetClient, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();
            Node = node;
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The node this neighbor belongs to
        /// </summary>
        protected IoNode<TJob> Node;

        /// <summary>
        /// Close this neighbor
        /// </summary>
        public override bool Close()
        {
            if (base.Close())
            {
                _logger.Info($"Closing neighbor `{Description}', Id = {Id}");
                return true;
            }

            return false;
        }

        /// <summary>
        /// The Id of this neighbor
        /// </summary>
        /// <returns></returns>
        public virtual string Id => Source.Key;
    }
}
