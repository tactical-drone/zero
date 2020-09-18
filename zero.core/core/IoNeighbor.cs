﻿using System;
using System.Threading.Tasks;
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
        /// <param name="mallocJob">The callback that allocates new message buffer space</param>
        /// <param name="producers">Nr of concurrent producers</param>
        /// <param name="consumers">Nr of concurrent consumers</param>
        public IoNeighbor(IoNode<TJob> node, IoNetClient<TJob> ioNetClient, Func<object, IoLoad<TJob>> mallocJob, int producers = 1, int consumers = 1)
            : base($"neighbor({ioNetClient?.Description})", ioNetClient, mallocJob, false, producers, consumers)
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
        /// The Id of this neighbor
        /// </summary>
        /// <returns></returns>
        public virtual string Id => Source.Key;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            Node = null;
#endif
        }

        /// <summary>
        /// 
        /// </summary>
        protected override async Task ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }
    }
}
