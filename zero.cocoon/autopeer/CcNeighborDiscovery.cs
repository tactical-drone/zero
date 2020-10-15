using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.misc;
using zero.core.network.ip;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Used by <see cref="CcNode"/> to discover other nodes
    /// </summary>
    public class CcNeighborDiscovery : IoNode<CcPeerMessage>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ccNode">The node this service belongs to</param>
        /// <param name="address">The listening address of this service</param>
        /// <param name="mallocNeighbor">Allocates neighbors on connection</param>
        /// <param name="prefetch">TCP job read ahead</param>
        /// <param name="concurrencyLevel">Nr of consumers that run concurrently</param>
        public CcNeighborDiscovery(CcNode ccNode, IoNodeAddress address,
            Func<IoNode<CcPeerMessage>, IoNetClient<CcPeerMessage>, object, IoNeighbor<CcPeerMessage>> mallocNeighbor, int prefetch, int concurrencyLevel) : base(address, mallocNeighbor, prefetch, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            CcNode = ccNode;
        }

        /// <summary>
        /// the logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Description
        /// </summary>
        public override string Description => $"`discovery({Address})'";

        /// <summary>
        /// The cocoon node this discovery service belongs to 
        /// </summary>
        public CcNode CcNode;

        /// <summary>
        /// Services Proxy request helper
        /// </summary>
        public CcService Services => CcNode.Services;
        
        /// <summary>
        /// The router
        /// </summary>
        public CcNeighbor Router { get; protected set; }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            CcNode = null;
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

        protected override async Task SpawnListenerAsync(Func<IoNeighbor<CcPeerMessage>, Task<bool>> acceptConnection = null, Func<Task> bootstrapAsync = null)
        {
            await base.SpawnListenerAsync(async neighbor =>
            {
                Router ??= (CcNeighbor) neighbor;
                return acceptConnection == null || await acceptConnection(neighbor).ConfigureAwait(false);
            }, bootstrapAsync).ConfigureAwait(false);
        }
    }
}
