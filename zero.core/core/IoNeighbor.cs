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
        /// ConstructAsync
        /// </summary>
        /// <param name="node">The node this neighbor is connected to</param>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocJob">The callback that allocates new message buffer space</param>
        /// <param name="enableSync"></param>
        /// <param name="producers">Nr of concurrent producers</param>
        /// <param name="consumers">Nr of concurrent consumers</param>
        public IoNeighbor(IoNode<TJob> node, IoNetClient<TJob> ioNetClient, Func<object, IoSink<TJob>> mallocJob,
            bool enableSync, int producers = 1, int consumers = 1)
            : base($"neighbor({ioNetClient?.Description})", ioNetClient, mallocJob, enableSync, true, producers: producers, consumers: consumers)
        {
            _logger = LogManager.GetCurrentClassLogger();
            Node = node;
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// The node this neighbor belongs to
        /// </summary>
        protected IoNode<TJob> Node;

        /// <summary>
        /// The Id of this neighbor
        /// </summary>
        /// <returns></returns>
        public virtual string Key => Source.Key;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            Node = null;
#endif
        }

        /// <summary>
        /// 
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }
    }
}
