using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Org.BouncyCastle.Utilities;
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
    public class CcDrone : IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>
    {
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="adjunct">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        /// <param name="concurrencyLevel"></param>
        public CcDrone(IoNode<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> node, CcAdjunct adjunct,
            IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> ioNetClient, int concurrencyLevel = 1)
            : base(node, ioNetClient, (o,s) => new CcWhispers("gossip rx", $"{ioNetClient.IoNetSocket.RemoteNodeAddress}", ioNetClient), false)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Adjunct = adjunct;

            //Testing
            var rand = new Random((int) DateTimeOffset.Now.Ticks);

            var t = ZeroAsync(static async @this  =>
            {
                while (!@this.Zeroed())
                {
                    await Task.Delay(@this.parm_insane_checks_delay * 1000, @this.AsyncTasks.Token).ConfigureAwait(false);
                    if (!@this.Zeroed() && @this.Adjunct == null || @this.Adjunct?.Direction == CcAdjunct.Heading.Undefined || @this.Adjunct?.State < CcAdjunct.AdjunctState.Connected && @this.Adjunct?.Direction != CcAdjunct.Heading.Undefined && @this.Adjunct.IsDroneConnected)
                    {
                        if(!@this.Zeroed() && @this.Adjunct == null)
                            @this._logger.Debug($"! {@this.Description} - n = {@this.Adjunct}, d = {@this.Adjunct?.Direction}, s = {@this.Adjunct?.State} (wants {CcAdjunct.AdjunctState.Connected}), {@this.Adjunct?.MetaDesc}");
                        await @this.ZeroAsync(new IoNanoprobe($"Invalid state after {@this.parm_insane_checks_delay}: s = {@this.Adjunct?.State}, wants = {CcAdjunct.AdjunctState.Connected}), {@this.Adjunct?.MetaDesc}")).FastPath().ConfigureAwait(false);
                    }
                }
            },this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach| TaskCreationOptions.PreferFairness);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private Logger _logger;

        
        /// <summary>
        /// Description
        /// </summary>
        private string _description;
        
        public override string Description
        {
            get
            {
                //if (_lastDescGen.CurrentMsDelta() > 100 && _description != null)
                //    return _description;
                
                //_lastDescGen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    return _description = $"`drone({(Source.IsOperational?"Active":"Zombie")} {(_assimulated? "Participant" : "Bystander")} [{Adjunct.Hub.Designation.IdString()}, {Adjunct.Designation.IdString()}], {Adjunct.Direction}, {IoSource.Key}, up = {TimeSpan.FromMilliseconds(Uptime.ElapsedMs())}'";
                }
                catch
                {
                    return _description = $"`drone({(Source?.IsOperational??false ? "Active":"Zombie")} {(_assimulated ? "Participant" : "Bystander")}, [{Adjunct?.Hub?.Designation?.IdString()}, {Adjunct?.Designation?.IdString()}], {IoSource?.Key}, up = {TimeSpan.FromMilliseconds(Uptime.ElapsedMs())}'";
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
        //         return $"`peer({Neighbor?.Direction.ToString().PadLeft(CcNeighbor.Heading.IsEgress.ToString().Length)} - {(Source?.IsOperational??false?"Connected":"Zombie")}) {Key}'";
        //         
        //     }
        // }

        /// <summary>
        /// The source
        /// </summary>
        public new IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> IoSource => (IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>) Source;

        /// <summary>
        /// The attached neighbor
        /// </summary>
        public CcAdjunct Adjunct { get; protected internal set; }

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
        protected IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> IoNetClient;


        /// <summary>
        /// Whether this drone ever formed part of a collective
        /// </summary>
        private bool _assimulated;

#if DEBUG
        /// <summary>
        /// Grace time for sanity checks
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_insane_checks_delay = 30;
#else
        /// <summary>
        /// Grace time for sanity checks
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_insane_checks_delay = 1;
#endif

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
            try
            {
                await DetachNeighborAsync().FastPath().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.Trace(e, $"{Description}");
            }

            try
            {
                if ((Adjunct?.Assimilated??false) && Uptime.ElapsedMs() > parm_min_uptime)
                    _logger.Info($"- {Description}, from: {ZeroedFrom?.Description}");
            }
            catch
            {
                // ignored
            }


            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
        }


        public new ValueTask<bool> ZeroAsync(IIoNanite from)
        {
            return base.ZeroAsync(from);
        }

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="adjunct"></param>
        /// <param name="direction"></param>
        public async ValueTask<bool> AttachViaAdjunctAsync(CcAdjunct.Heading direction)
        {
            //Raced?
            if (Adjunct.IsDroneAttached)
                return false;

            //Attach the other way
            var attached = await Adjunct.AttachPeerAsync(this, direction).FastPath().ConfigureAwait(false);

            if (attached)
            {
                _logger.Trace($"{nameof(AttachViaAdjunctAsync)}: {direction} attach to neighbor {Adjunct.Description}");
                _assimulated = true;
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
            var state = ValueTuple.Create(this, latch);
            await ZeroAtomicAsync(static (_, state, _) =>
            {
                var (@this, latch) = state;
                if (@this.Adjunct != null)
                    state.Item2 = @this.Adjunct;
                
                return new ValueTask<bool>(true);
            },state).FastPath().ConfigureAwait(false);

            if(state.Item2 != null)
                await state.Item2.DetachPeerAsync().FastPath().ConfigureAwait(false);
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
            
                //if (Adjunct?.Direction == CcAdjunct.Heading.IsEgress)
                {

                    var vb = new byte[8];
                    Write(vb.AsSpan(), ref v);

                    var m = new CcWhisperMsg() {Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(vb))};

                    var buf = m.ToByteArray();

                    if (!Zeroed())
                    {
                        if(await ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)Source).IoNetSocket.SendAsync(buf, 0, buf.Length).FastPath().ConfigureAwait(false) > 0)
                        {
                            //Interlocked.Increment(ref AccountingBit);
                            await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Adjunct.CcCollective.Hub.Router.Designation.IdString(),
                                    Id = Adjunct.Designation.IdString(),
                                    Type = "gossip"
                                }
                            }).FastPath().ConfigureAwait(false);
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
