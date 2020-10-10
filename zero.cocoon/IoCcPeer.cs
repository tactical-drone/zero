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
            : base(node, ioNetClient, userData => new IoCcGossipMessage("gossip rx", $"{ioNetClient.IoNetSocket.RemoteNodeAddress}", ioNetClient), ioNetClient.ConcurrencyLevel, ioNetClient.ConcurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Neighbor = neighbor;
            //if(Neighbor != null)
            //    AttachNeighborAsync(Neighbor);


            //Testing
            var rand = new Random((int) DateTimeOffset.Now.Ticks);
#pragma warning disable 1998
            Task.Run(async () =>
#pragma warning restore 1998
            {
                return;
                
//                await Task.Delay(rand.Next(60000000), AsyncTasks.Token).ContinueWith(r =>
//                //await Task.Delay(rand.Next(30000), AsyncT
//                //asks.Token).ContinueWith(r =>
//                {
//                    if (r.IsCompletedSuccessfully && !Zeroed())
//                    {
//                        _logger.Fatal($"Testing SOCKET FAILURE {Id}");
//
//                        Source.ZeroAsync(this);
//
//                        GC.Collect(GC.MaxGeneration);
//                    }
//                });
            }).ConfigureAwait(false);
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
                return $"`peer({Neighbor?.Direction.ToString().PadLeft(IoCcNeighbor.Kind.OutBound.ToString().Length)} - {(Source?.IsOperational??false?"Connected":"Zombie")}) {Key}'";
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
        public async ValueTask<bool> AttachNeighborAsync(IoCcNeighbor neighbor, IoCcNeighbor.Kind direction)
        {

            Neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)} cannot be null");
            
            //Attach the other way
            var attached = await Neighbor.AttachPeerAsync(this, direction).ZeroBoostAsync().ConfigureAwait(false);

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
            
                if (Neighbor?.Direction == IoCcNeighbor.Kind.OutBound)
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
