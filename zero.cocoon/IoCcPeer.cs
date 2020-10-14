using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.misc;
using static System.Runtime.InteropServices.MemoryMarshal;

namespace zero.cocoon
{
    public class IoCcPeer : IoNeighbor<IoCcGossipMessage>
    {
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="neighbor">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        public IoCcPeer(IoNode<IoCcGossipMessage> node, IoCcNeighbor neighbor,
            IoNetClient<IoCcGossipMessage> ioNetClient)
            : base(node, ioNetClient,
                userData => new IoCcGossipMessage("gossip rx", $"{ioNetClient.IoNetSocket.RemoteNodeAddress}",
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
                    if (!Zeroed() && Neighbor == null || Neighbor?.Direction == IoCcNeighbor.Heading.Undefined || Neighbor?.State < IoCcNeighbor.NeighborState.Connected)
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


        private string _description;

        /// <summary>
        /// A description of this peer
        /// </summary>
        public override string Description
        {
            get
            {
                //if (_description != null)
                //    return _description;
                return $"`peer({Neighbor?.Direction.ToString().PadLeft(IoCcNeighbor.Heading.Egress.ToString().Length)} - {(Source?.IsOperational??false?"Connected":"Zombie")}) {Key}'";
                
            }
        }

        /// <summary>
        /// The source
        /// </summary>
        public new IoNetClient<IoCcGossipMessage> IoSource => (IoNetClient<IoCcGossipMessage>) Source;

        /// <summary>
        /// The attached neighbor
        /// </summary>
        public IoCcNeighbor Neighbor { get; private set; }

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
        protected IoNetClient<IoCcGossipMessage> IoNetClient;

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
        public async ValueTask<bool> AttachNeighborAsync(IoCcNeighbor neighbor, IoCcNeighbor.Heading direction)
        {

            Neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)} cannot be null");
            
            //Attach the other way
            var attached = await Neighbor.AttachPeerAsync(this, direction).ConfigureAwait(false);

            if (attached)
            {
                _logger.Trace($"{nameof(AttachNeighborAsync)}: {direction} attach to neighbor {neighbor.Description}");

                StartTestModeAsync();
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
            IoCcNeighbor neighbor = null;

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
                if (Interlocked.Read(ref ((IoCcNode) Node).Testing) == 0)
                    return;

                if (Interlocked.Read(ref _isTesting) > 0)
                    return;

                if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                    return;
            
                if (Neighbor?.Direction == IoCcNeighbor.Heading.Egress)
                {
                    long v = 0;
                    var vb = new byte[8];
                    Write(vb.AsSpan(), ref v);

                    if (!Zeroed())
                    {
                        if (await((IoNetClient<IoCcGossipMessage>) Source).IoNetSocket.SendAsync(vb, 0, vb.Length).ConfigureAwait(false) > 0)
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
