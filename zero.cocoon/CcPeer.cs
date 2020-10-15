using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.core.core;
using zero.core.misc;
using zero.core.network.ip;
using static System.Runtime.InteropServices.MemoryMarshal;

namespace zero.cocoon
{
    public class CcPeer : IoNeighbor<CcGossipMessage>
    {
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="neighbor">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        public CcPeer(IoNode<CcGossipMessage> node, CcNeighbor neighbor,
            IoNetClient<CcGossipMessage> ioNetClient)
            : base(node, ioNetClient,
                userData => new CcGossipMessage("gossip rx", $"{ioNetClient.IoNetSocket.RemoteNodeAddress}",
                    ioNetClient), ioNetClient.ConcurrencyLevel, ioNetClient.ConcurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Neighbor = neighbor;
            //if(Neighbor != null)
            //    AttachNeighborAsync(Neighbor);


            //Testing
            var rand = new Random((int) DateTimeOffset.Now.Ticks);

            var t = Task.Run(async () =>
            {
                while (!Zeroed())
                {
                    await Task.Delay(60000, AsyncTasks.Token);
                    if (!Zeroed() && Neighbor == null || Neighbor?.Direction == CcNeighbor.Heading.Undefined || Neighbor?.State < CcNeighbor.NeighborState.Connected)
                    {
                        _logger.Fatal($"! {Description} - n = {Neighbor}, d = {Neighbor?.Direction}, s = {Neighbor?.State}");
                        await ZeroAsync(this);
                    }
                }
            });
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        
        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        /// <summary>
        /// rate limit desc gens
        /// </summary>
        private long _lastDescGen = (DateTimeOffset.UtcNow + TimeSpan.FromDays(1)).Millisecond;

        public override string Description
        {
            get
            {
                if (_lastDescGen.ElapsedMsDelta() > 10000 && _description != null)
                    return _description;
                
                _lastDescGen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    return _description = $"`peer({Neighbor?.Direction.ToString().PadLeft(CcNeighbor.Heading.Egress.ToString().Length)} - {(Source?.IsOperational??false?"Connected":"Zombie")}) {IoSource.Key}, [{Neighbor?.Identity.IdString()}]'";
                }
                catch
                {
                    return _description??Key;
                }
            }
        }
        

        // private string _description;
        //
        // /// <summary>
        // /// A description of this peer
        // /// </summary>
        // public override string Description
        // {
        //     get
        //     {
        //         //if (_description != null)
        //         //    return _description;
        //         return $"`peer({Neighbor?.Direction.ToString().PadLeft(CcNeighbor.Heading.Egress.ToString().Length)} - {(Source?.IsOperational??false?"Connected":"Zombie")}) {Key}'";
        //         
        //     }
        // }

        /// <summary>
        /// The source
        /// </summary>
        public new IoNetClient<CcGossipMessage> IoSource => (IoNetClient<CcGossipMessage>) Source;

        /// <summary>
        /// The attached neighbor
        /// </summary>
        public CcNeighbor Neighbor { get; private set; }

        /// <summary>
        /// CcId
        /// </summary>
        public override string Key => Neighbor?.Key ?? "null";

        /// <summary>
        /// Used for testing
        /// </summary>
        public long AccountingBit = 0;

        /// <summary>
        /// Helper
        /// </summary>
        protected IoNetClient<CcGossipMessage> IoNetClient;

        /// <summary>
        /// 
        /// </summary>
        private long _isTesting = 0;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            IoNetClient = null;
            Neighbor = null;
#endif
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            if((Neighbor?.ConnectedAtLeastOnce??false) && Source.IsOperational)
                _logger.Info($"- {Description}, from {ZeroedFrom?.Description}");

            await DetachNeighborAsync().ConfigureAwait(false);
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="neighbor"></param>
        /// <param name="direction"></param>
        public async ValueTask<bool> AttachNeighborAsync(CcNeighbor neighbor, CcNeighbor.Heading direction)
        {

            Neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)} cannot be null");
            
            //Attach the other way
            var attached = await Neighbor.AttachPeerAsync(this, direction).ConfigureAwait(false);

            if (attached)
            {
                _logger.Trace($"{nameof(AttachNeighborAsync)}: {direction} attach to neighbor {neighbor.Description}");

#pragma warning disable 4014
                StartTestModeAsync();
#pragma warning restore 4014
            }
            else
            {
                _logger.Trace($"{nameof(AttachNeighborAsync)}: [RACE LOST]{direction} attach to neighbor {neighbor.Description}, {neighbor.MetaDesc}");
            }

            return attached;
        }

        /// <summary>
        /// Detaches current neighbor
        /// </summary>
        public async Task DetachNeighborAsync()
        {
            CcNeighbor neighbor = null;

            lock (this)
            {
                if (Neighbor != null)
                {
                    neighbor = Neighbor;
                    Neighbor = null;
                }
            }

            if(neighbor != null)
                await neighbor.DetachPeerAsync(this).ConfigureAwait(false);
        }

        /// <summary>
        /// A test mode
        /// </summary>
        public async Task StartTestModeAsync()
        {
            try
            {
                if (Interlocked.Read(ref ((CcNode) Node).Testing) == 0)
                    return;

                if (Interlocked.Read(ref _isTesting) > 0)
                    return;

                if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                    return;
            
                if (Neighbor?.Direction == CcNeighbor.Heading.Egress)
                {
                    long v = 0;
                    var vb = new byte[8];
                    Write(vb.AsSpan(), ref v);

                    if (!Zeroed())
                    {
                        if (await((IoNetClient<CcGossipMessage>) Source).IoNetSocket.SendAsync(vb, 0, vb.Length).ConfigureAwait(false) > 0)
                        {
                            Interlocked.Increment(ref AccountingBit);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Trace(e,Description);
            }
        }
    }
}
