using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.core.conf;
using zero.core.core;
using zero.core.feat.models.protobuffer;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;
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
            IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> ioNetClient)
            : base(node, ioNetClient, static (o,s) => new CcWhispers("gossip rx", $"{((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)s).IoNetSocket.RemoteNodeAddress}", ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)s)), true)
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Adjunct = adjunct;

            //Testing
            var rand = new Random(DateTimeOffset.Now.Ticks.GetHashCode() * DateTimeOffset.Now.Ticks.GetHashCode());

            var t = ZeroAsync(static async @this  =>
            {
                while (!@this.Zeroed())
                {
                    await Task.Delay(@this.parm_insane_checks_delay_s * 1000, @this.AsyncTasks.Token).ConfigureAwait(@this.Zc);
                    if (!@this.Zeroed() && @this.Adjunct == null || @this.Adjunct?.Direction == CcAdjunct.Heading.Undefined || @this.Adjunct?.State < CcAdjunct.AdjunctState.Connected && @this.Adjunct?.Direction != CcAdjunct.Heading.Undefined && @this.Adjunct.IsDroneConnected)
                    {
                        if (!@this.Zeroed() && @this.Adjunct == null)
                        {
                            @this._logger.Debug($"! {@this.Description} - n = {@this.Adjunct}, d = {@this.Adjunct?.Direction}, s = {@this.Adjunct?.State} (wants {CcAdjunct.AdjunctState.Connected}), {@this.Adjunct?.MetaDesc}");
                        }
                        @this.Zero(new IoNanoprobe($"Invalid state after {@this.parm_insane_checks_delay_s}: s = {@this.Adjunct?.State}, wants = {CcAdjunct.AdjunctState.Connected}), {@this.Adjunct?.MetaDesc}"));
                    }

                    if (@this.Adjunct != null) @this.Adjunct.WasAttached = true;
                }
            },this, TaskCreationOptions.DenyChildAttach);

            _m = new CcWhisperMsg() { Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(_vb)) };
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
        public int parm_insane_checks_delay_s = 30;
#else
        /// <summary>
        /// Grace time for sanity checks
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_insane_checks_delay_s = 10;
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
                await DetachNeighborAsync().FastPath().ConfigureAwait(Zc);
            }
            catch (Exception e)
            {
                _logger.Trace(e, $"{Description}");
            }

            try
            {
                if ((Adjunct?.Assimilated??false) && Uptime.ElapsedMs() > parm_min_uptime_ms)
                    _logger.Info($"- {Description}, from: {ZeroedFrom?.Description}");
            }
            catch
            {
                // ignored
            }


            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
        }

        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed();
        }

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="adjunct"></param>
        /// <param name="direction"></param>
        public async ValueTask<bool> AttachViaAdjunctAsync(CcAdjunct.Heading direction)
        {
            try
            {
                //Raced?
                if (Adjunct.IsDroneAttached)
                {
                    return false;
                }
                    

                //Attach the other way
                var attached = await Adjunct.AttachDroneAsync(this, direction).FastPath().ConfigureAwait(Zc);

                if (attached)
                {
                    _logger.Trace($"{nameof(AttachViaAdjunctAsync)}: {direction} attach to adjunct {Adjunct.Description}");
                    _assimulated = true;
                }
                else
                {
                    _logger.Trace($"{nameof(AttachViaAdjunctAsync)}: [RACE LOST]{direction} attach to adjunct {Adjunct.Description}, {Adjunct.MetaDesc}");
                }

                return attached;
            }
            catch (Exception) when (Zeroed()) { }
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"");                               
            }            
            return false; 
        }

        /// <summary>
        /// Detaches current neighbor
        /// </summary>
        public async ValueTask DetachNeighborAsync()
        {
            if (Adjunct != null)
            {
                await Adjunct.DetachDroneAsync().FastPath().ConfigureAwait(Zc);
                Adjunct = null;
            }
        }

        /// <summary>
        /// A test mode
        /// </summary>
        /// 
        private readonly byte[] _vb = new byte[8];
        private readonly CcWhisperMsg _m;
        public async ValueTask EmitTestGossipMsgAsync(long v)
        {
            try
            {
                if (Interlocked.Read(ref ((CcCollective) Node).Testing) == 0)
                    return;

                if(!Source.IsOperational || Adjunct.CcCollective.TotalConnections < 1)
                    return;

                //if (Interlocked.Read(ref _isTesting) > 0)
                //    return;

                //if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                //    return;
            
                //if (Adjunct?.Direction == CcAdjunct.Heading.IsEgress)
                {
                    
                    Write(_vb.AsSpan(), ref v);

                    //var m = new CcWhisperMsg() {Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(vb))};

                    var buf = _m.ToByteArray();

                    if (!Zeroed())
                    {
                        var dupEndpoints = Adjunct.CcCollective.DupHeap.Take(IoSource.IoNetSocket.LocalAddress);

                        if (dupEndpoints == null)
                            throw new OutOfMemoryException(
                                $"{Adjunct.CcCollective.DupHeap}: {Adjunct.CcCollective.DupHeap.ReferenceCount}/{Adjunct.CcCollective.DupHeap.MaxSize} - c = {Adjunct.CcCollective.DupChecker.Count}, m = _maxReq");

                        if (!Adjunct.CcCollective.DupChecker.TryAdd(v, dupEndpoints))
                        {
                            dupEndpoints.ZeroManaged();
                            Adjunct.CcCollective.DupHeap.Return(dupEndpoints);

                            //best effort
                            if (Adjunct.CcCollective.DupChecker.TryGetValue(v, out dupEndpoints))
                            {
                                dupEndpoints.Add(IoSource.IoNetSocket.LocalAddress.GetHashCode(), true);
                            }
                        }

                        Interlocked.Increment(ref AccountingBit);
                        Adjunct.CcCollective.IncEventCounter();

                        var socket = ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)Source).IoNetSocket;
                        if (socket.IsConnected() && await socket.SendAsync(buf, 0, buf.Length, timeout: 20).FastPath().ConfigureAwait(Zc) > 0)
                        {                            
                            if (AutoPeeringEventService.Operational)
                                await AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
                                {
                                    EventType = AutoPeerEventType.SendProtoMsg,
                                    Msg = new ProtoMsg
                                    {
                                        CollectiveId = Adjunct.CcCollective.Hub.Router.Designation.IdString(),
                                        Id = Adjunct.Designation.IdString(),
                                        Type = "gossip"
                                    }
                                }).FastPath().ConfigureAwait(Zc);
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
