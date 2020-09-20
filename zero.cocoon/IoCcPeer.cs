using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.core.core;
using zero.core.network.ip;
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
            : base(node, ioNetClient, userData => new IoCcGossipMessage("gossip rx", $"{ioNetClient.AddressString}", ioNetClient), 1,1)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Neighbor = neighbor;
            //if(Neighbor != null)
            //    AttachNeighbor(Neighbor);


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
                if (_description != null)
                    return _description;
                return _description = $"`peer({Neighbor?.Direction}) {Id}'";
            }
        }

        /// <summary>
        /// The attached neighbor
        /// </summary>
        public IoCcNeighbor Neighbor { get; private set; }

        private string _id;
        /// <summary>
        /// CcId
        /// </summary>
        public override string Id
        {
            get
            {
                if (_id != null)
                    return _id;
                return _id = Neighbor?.Id ?? "null";
            }
        }

        /// <summary>
        /// Used for testing
        /// </summary>
        public long AccountingBit = 0;

        /// <summary>
        /// Helper
        /// </summary>
        protected IoNetClient<IoCcGossipMessage> IoNetClient;

        private long _isTesting = 0;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
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
        protected override async Task ZeroManagedAsync()
        {
            await DetachNeighborAsync().ConfigureAwait(false);
            await Source.ZeroAsync(this).ConfigureAwait(false);
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="neighbor"></param>
        /// <param name="direction"></param>
        public bool AttachNeighbor(IoCcNeighbor neighbor, IoCcNeighbor.Kind direction)
        {

            Neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)} cannot be null");
            
            //Attach the other way
            var attached = Neighbor.AttachPeer(this, direction);

            if (attached)
            {
                _logger.Debug($"{nameof(AttachNeighbor)}: {direction} attach to neighbor {neighbor.Description}");

                StartTestModeAsync();
            }
            else
            {
                _logger.Trace($"{nameof(AttachNeighbor)}: [RACE LOST]{direction} attach to neighbor {neighbor.Description}, {neighbor.MetaDesc}");
            }

            return attached;
        }

        /// <summary>
        /// Detaches current neighbor
        /// </summary>
        public async Task DetachNeighborAsync()
        {
            if(Neighbor != null)
                await Neighbor.DetachPeerAsync().ConfigureAwait(false);
            Neighbor = null;
        }

        /// <summary>
        /// A test mode
        /// </summary>
        public async Task StartTestModeAsync()
        {
            if (Interlocked.Read(ref ((IoCcNode) Node).Testing) == 0)
                return;

            if (Interlocked.Read(ref _isTesting) > 0)
                return;

            if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                return;
            
            if (Neighbor.Direction == IoCcNeighbor.Kind.OutBound)
            {
                long v = 0;
                var vb = new byte[8];
                Write(vb.AsSpan(), ref v);

                if (!Zeroed())
                {
                    if (await((IoNetClient<IoCcGossipMessage>) Source).Socket.SendAsync(vb, 0, vb.Length).ConfigureAwait(false) > 0)
                    {
                        Interlocked.Increment(ref AccountingBit);
                    }
                }
            }
        }
    }
}
