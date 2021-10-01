﻿using System;
using System.Threading.Tasks;
using NLog;
using Proto;
using zero.cocoon.models.batches;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.models.protobuffer;
using zero.core.network.ip;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Used by <see cref="CcCollective"/> to discover other nodes
    /// </summary>
    public class CcHub : IoNode<CcProtocMessage<Packet, CcDiscoveryBatch>>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ccCollective">The node this service belongs to</param>
        /// <param name="address">The listening address of this service</param>
        /// <param name="mallocNeighbor">Allocates neighbors on connection</param>
        /// <param name="prefetch">TCP job read ahead</param>
        /// <param name="concurrencyLevel">Nr of consumers that run concurrently</param>
        public CcHub(CcCollective ccCollective, IoNodeAddress address,
            Func<IoNode<CcProtocMessage<Packet, CcDiscoveryBatch>>,
                IoNetClient<CcProtocMessage<Packet, CcDiscoveryBatch>>, object,
                IoNeighbor<CcProtocMessage<Packet, CcDiscoveryBatch>>> mallocNeighbor, int prefetch,
            int concurrencyLevel) : base(address, mallocNeighbor, prefetch, concurrencyLevel, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            CcCollective = ccCollective;
        }

        /// <summary>
        /// the logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        public override string Description => $"`discovery({Address})'";

        /// <summary>
        /// The cocoon node this discovery service belongs to 
        /// </summary>
        public CcCollective CcCollective;

        /// <summary>
        /// Services Proxy request helper
        /// </summary>
        public CcService Services => CcCollective.Services;
        
        /// <summary>
        /// The router
        /// </summary>
        public CcAdjunct Router { get; protected set; }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _logger = null;
            CcCollective = null;
            Router = null;
#endif
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }

        protected override async ValueTask SpawnListenerAsync(Func<IoNeighbor<CcProtocMessage<Packet, CcDiscoveryBatch>>, ValueTask<bool>> acceptConnection = null, Func<ValueTask> bootstrapAsync = null)
        {
            await base.SpawnListenerAsync(async router =>
            {
                Router ??= (CcAdjunct) router;
                return acceptConnection == null || await acceptConnection(router).ConfigureAwait(false);
            }, bootstrapAsync).ConfigureAwait(false);
        }
    }
}
