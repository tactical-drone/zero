using System;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.feat.models.protobuffer;
using zero.core.network.ip;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Used by <see cref="CcCollective"/> to discover other nodes
    /// </summary>
    public class CcHub : IoNode<CcProtocMessage<chroniton, CcDiscoveryBatch>>
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
            Func<IoNode<CcProtocMessage<chroniton, CcDiscoveryBatch>>,
                IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>, object,
                IoNeighbor<CcProtocMessage<chroniton, CcDiscoveryBatch>>> mallocNeighbor, int prefetch,
            int concurrencyLevel) : base(address, mallocNeighbor, prefetch, concurrencyLevel, ccCollective.MaxAdjuncts + 1)//TODO config
        {
            CcCollective = ccCollective;
            Designation = ccCollective.CcId;
        }

        /// <summary>
        /// Description
        /// </summary>
        public override string Description => $"`discovery({Address})'";

        /// <summary>
        /// The hub id
        /// </summary>
        public CcDesignation Designation { get; }

        /// <summary>
        /// The cocoon node this discovery service belongs to 
        /// </summary>
        public readonly CcCollective CcCollective;

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
            Router = null;
#endif
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            LogManager.GetCurrentClassLogger().Info($"- {Description}, from = {ZeroedFrom}, reason = {ZeroReason}");
            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// Configure the UDP proxy router
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="acceptConnection"></param>
        /// <param name="context"></param>
        /// <param name="bootFunc"></param>
        /// <returns></returns>
        protected override ValueTask SpawnListenerAsync<T,TContext>(Func<IoNeighbor<CcProtocMessage<chroniton, CcDiscoveryBatch>>, T,ValueTask<bool>> acceptConnection = null, T context = default, Func<TContext,ValueTask> bootFunc = null, TContext bootData = default)
        {
            return base.SpawnListenerAsync(static async (router, state) =>
            {
                var (@this, nanite, acceptConnection) = state;
                @this.Router ??= (CcAdjunct)router;
                return acceptConnection == null || await acceptConnection(router,nanite).FastPath();
            }, ValueTuple.Create(this, context, acceptConnection), bootFunc, bootData);
        }
    }
}
