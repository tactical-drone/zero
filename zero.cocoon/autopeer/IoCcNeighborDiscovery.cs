using System;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.network.ip;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Used by <see cref="CcNode"/> to discover other nodes
    /// </summary>
    public class IoCcNeighborDiscovery : IoNode<IoCcPeerMessage>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ioCcNode">The node this service belongs to</param>
        /// <param name="address">The listening address of this service</param>
        /// <param name="mallocNeighbor">Allocates neighbors on connection</param>
        /// <param name="prefetch">TCP job read ahead</param>
        /// <param name="concurrencyLevel">Nr of consumers that run concurrently</param>
        public IoCcNeighborDiscovery(IoCcNode ioCcNode, IoNodeAddress address,
            Func<IoNode<IoCcPeerMessage>, IoNetClient<IoCcPeerMessage>, object, IoNeighbor<IoCcPeerMessage>> mallocNeighbor, int prefetch, int concurrencyLevel) : base(address, mallocNeighbor, prefetch, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            CcNode = ioCcNode;
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
        public IoCcNode CcNode;

        /// <summary>
        /// Services Proxy request helper
        /// </summary>
        public IoCcService Services => CcNode.Services;

        public IoCcNeighbor LocalNeighbor { get; protected set; }

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            CcNode = null;
            LocalNeighbor = null;
#endif
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            //foreach (var neighborsValue in Neighbors.Values)
            //    await neighborsValue.ZeroAsync(this);

            await LocalNeighbor.ZeroAsync(this);

            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        protected override async Task SpawnListenerAsync(Func<IoNeighbor<IoCcPeerMessage>, Task<bool>> acceptConnection = null, Func<Task> bootstrapAsync = null)
        {
            await base.SpawnListenerAsync(async neighbor =>
            {
                LocalNeighbor ??= (IoCcNeighbor) neighbor;
                return acceptConnection == null || await acceptConnection(neighbor).ConfigureAwait(false);
            }, bootstrapAsync).ConfigureAwait(false);
        }
    }
}
