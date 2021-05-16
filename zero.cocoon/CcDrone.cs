using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.core.conf;
using zero.core.core;
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.network.ip;
using zero.core.patterns.misc;
using static System.Runtime.InteropServices.MemoryMarshal;

namespace zero.cocoon
{
    public class CcDrone : IoNeighbor<CcProtocMessage<CcWisperMsg, CcGossipBatch>>
    {
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="adjunct">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        /// <param name="concurrencyLevel"></param>
        public CcDrone(IoNode<CcProtocMessage<CcWisperMsg, CcGossipBatch>> node, CcAdjunct adjunct,
            IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>> ioNetClient, int concurrencyLevel = 1)
            : base(node, ioNetClient,
                userData => new CcWispers("gossip rx", $"{ioNetClient.IoNetSocket.RemoteNodeAddress}",
                    ioNetClient), ioNetClient.ConcurrencyLevel, ioNetClient.ConcurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Adjunct = adjunct;

            //Testing
            var rand = new Random((int) DateTimeOffset.Now.Ticks);

            var t = Task.Factory.StartNew(async () =>
            {
                while (!Zeroed())
                {
                    await Task.Delay(parm_insane_checks_delay * 1000, AsyncTasks.Token);
                    if (!Zeroed() && Adjunct == null || Adjunct?.Direction == CcAdjunct.Heading.Undefined || Adjunct?.State < CcAdjunct.AdjunctState.Peering)
                    {
                        _logger.Debug($"! {Description} - n = {Adjunct}, d = {Adjunct?.Direction}, s = {Adjunct?.State}, {Adjunct?.MetaDesc}");
                        await ZeroAsync(new IoNanoprobe($"Invalid state after {parm_insane_checks_delay}: {Adjunct?.MetaDesc}")).ConfigureAwait(false);
                    }
                }
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private Logger _logger;

        
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
                //if (_lastDescGen.ElapsedMsDelta() > 100 && _description != null)
                //    return _description;
                
                //_lastDescGen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    return _description = $"`drone({Adjunct?.Direction.ToString().PadLeft(CcAdjunct.Heading.Ingress.ToString().Length)} - {(Source?.IsOperational??false?"Connected":"Zombie")}) {IoSource.Key}, [{Adjunct?.Designation.IdString()}]'";
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
        public new IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>> IoSource => (IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>>) Source;

        /// <summary>
        /// The attached neighbor
        /// </summary>
        public CcAdjunct Adjunct { get; set; }

        /// <summary>
        /// CcId
        /// </summary>
        public override string Key => Adjunct?.Key ?? "null";

        /// <summary>
        /// Used for testing
        /// </summary>
        public long AccountingBit = 0;

        /// <summary>
        /// Helper
        /// </summary>
        protected IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>> IoNetClient;

        /// <summary>
        /// 
        /// </summary>
        private long _isTesting = 0;

        /// <summary>
        /// Grace time for sanity checks
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_insane_checks_delay = 5;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _logger = null;
            IoNetClient = null;
            Adjunct = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            if((Adjunct?.Assimilated??false) && Uptime.TickSec() > parm_min_uptime)
                _logger.Info($"- {Description}, from: {ZeroedFrom?.Description}");

            await DetachNeighborAsync().ConfigureAwait(false);
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }


        public new ValueTask ZeroAsync(IIoNanite @from)
        {
            return base.ZeroAsync(@from);
        }

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="adjunct"></param>
        /// <param name="direction"></param>
        public async ValueTask<bool> AttachViaAdjunctAsync(CcAdjunct.Heading direction)
        {
            
            //Attach the other way
            var attached = await Adjunct.AttachPeerAsync(this, direction).ConfigureAwait(false);

            if (attached)
            {
                _logger.Trace($"{nameof(AttachViaAdjunctAsync)}: {direction} attach to neighbor {Adjunct.Description}");
                
                //var t = EmitTestGossipMsgAsync();
            }
            else
            {
                _logger.Trace($"{nameof(AttachViaAdjunctAsync)}: [RACE LOST]{direction} attach to neighbor {Adjunct.Description}, {Adjunct.MetaDesc}");
            }

            return attached;
        }

        /// <summary>
        /// Detaches current neighbor
        /// </summary>
        public async ValueTask DetachNeighborAsync()
        {
            CcAdjunct latch = null;

            lock (this)
            {
                if (Adjunct != null)
                {
                    latch = Adjunct;
                    Adjunct = null;
                }
            }

            if(latch != null)
                await latch.DetachPeerAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// A test mode
        /// </summary>
        public async ValueTask EmitTestGossipMsgAsync(long v)
        {
            try
            {
                if (Interlocked.Read(ref ((CcCollective) Node).Testing) == 0)
                    return;

                //if (Interlocked.Read(ref _isTesting) > 0)
                //    return;

                //if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                //    return;
            
                //if (Adjunct?.Direction == CcAdjunct.Heading.Egress)
                {

                    var vb = new byte[8];
                    Write(vb.AsSpan(), ref v);

                    var m = new CcWisperMsg() {Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(vb))};

                    var buf = m.ToByteArray();

                    if (!Zeroed())
                    {
                        var sentTask = ((IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>>) Source).IoNetSocket.SendAsync(buf, 0, buf.Length);
                        if (!sentTask.IsCompletedSuccessfully)
                            await sentTask.ConfigureAwait(false);

                        if(sentTask.Result > 0)
                        {
                            //Interlocked.Increment(ref AccountingBit);
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Adjunct.CcCollective.Hub.Router.Designation.IdString(),
                                    Id = Adjunct.Designation.IdString(),
                                    Type = "gossip"
                                }
                            });
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
