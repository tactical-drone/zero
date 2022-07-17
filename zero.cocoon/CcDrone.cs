using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using K4os.Compression.LZ4;
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
using zero.core.patterns.heap;
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
        public CcDrone(IoNode<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> node, CcAdjunct adjunct,
            IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> ioNetClient)
            : base
            (
                node,
                ioNetClient,
                static (ioZero, _) => new CcWhispers(string.Empty, string.Empty, ((CcDrone)ioZero)?.MessageService), false
            )
        {
            //sentinel
            if (Source == null)
                return;

            _logger = LogManager.GetCurrentClassLogger();

            Adjunct = adjunct;

            //Testing
            var rand = new Random(DateTimeOffset.Now.Ticks.GetHashCode() * DateTimeOffset.Now.Ticks.GetHashCode());

            //var t = ZeroAsync(static async @this  =>
            //{
            //    while (!@this.Zeroed())
            //    {
            //        await Task.Delay(@this.parm_insane_checks_delay_s * 1000, @this.AsyncTasks.Token);
            //        if (!@this.Zeroed() && @this.Adjunct == null || @this.Adjunct?.Direction == CcAdjunct.Heading.Undefined || @this.Adjunct?.State < CcAdjunct.AdjunctState.Connected && @this.Adjunct?.Direction != CcAdjunct.Heading.Undefined && @this.Adjunct.IsDroneConnected)
            //        {
            //            if (!@this.Zeroed() && @this.Adjunct == null)
            //            {
            //                @this._logger.Debug($"! {@this.Description} - n = {@this.Adjunct}, d = {@this.Adjunct?.Direction}, s = {@this.Adjunct?.State} (wants {CcAdjunct.AdjunctState.Connected}), {@this.Adjunct?.MetaDesc}");
            //            }
            //            await @this.DisposeAsync(@this, $"Invalid state after {@this.parm_insane_checks_delay_s}: s = {@this.Adjunct?.State}, wants = {CcAdjunct.AdjunctState.Connected}), {@this.Adjunct?.MetaDesc}");
            //        }
            //        else if (@this.Adjunct != null && @this.MessageService.IsOperational()) 
            //            @this.Adjunct.WasAttached = true;
            //    }
            //},this, TaskCreationOptions.DenyChildAttach);

            _m = new CcWhisperMsg() { Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(_vb)) };

            _sendBuf = new IoHeap<byte[]>($"{nameof(_sendBuf)}: {Description}", 16, (_, _) => new byte[32],true);
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
                //if (_lastDescGen.CurrentUtcMsDelta() > 100 && _description != null)
                //    return _description;
                
                //_lastDescGen = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    if(!Zeroed())
                        return _description = $"`drone({(!Zeroed()? "Active":"Zombie")} {(_assimulated? "Participant" : "Bystander")} [{Adjunct?.Hub.Designation.IdString()}, {Adjunct?.Designation.IdString()}], {Adjunct?.Direction},{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedMs())}'";
                    return _description = $"`drone({(!Zeroed()? "Active" : "Zombie")} {(_assimulated ? "Participant" : "Bystander")}, [{Adjunct?.Hub?.Designation?.IdString()}, {Adjunct?.Designation?.IdString()}], {MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedMs())}'";
                }
                catch
                {
                    return _description = $"`drone({(!Zeroed()? "Active":"Zombie")} {(_assimulated ? "Participant" : "Bystander")}, [{Adjunct?.Hub?.Designation?.IdString()}, {Adjunct?.Designation?.IdString()}], {MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedMs())}'";
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

        private volatile CcAdjunct _adjunct;
        /// <summary>
        /// The attached neighbor
        /// </summary>
        public CcAdjunct Adjunct {
            get => _adjunct;
            protected internal set => _adjunct = value;
        }

        public IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> MessageService => (IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)Source;

        private string _key;
        /// <summary>
        /// CcId
        /// </summary>
        public override string Key
        {
            get
            {
                if (_key != null)
                    return _key;
                if(Adjunct != null)
                    return _key = Adjunct.Key;
                return string.Empty;
            }
        }

        /// <summary>
        /// Used for testing
        /// </summary>
        public volatile bool AccountingBit = true;


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
        public int parm_insane_checks_delay_s = 1;
#endif

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            _logger = null;
            Adjunct = null;
            _sendBuf = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();
            try
            {
                await DropAdjunctAsync().FastPath();
            }
            catch (Exception e)
            {
                _logger.Trace(e, $"{Description}");
            }

            try
            {
                if ((Adjunct?.WasAttached??false) && UpTime.ElapsedMs() > parm_min_uptime_ms)
                    _logger.Info($"- {Description}, from: {ZeroedFrom?.Description}");

                await _sendBuf.ZeroManagedAsync<object>().FastPath();
            }
            catch
            {
                // ignored
            }
        }

        /// <summary>
        /// Zeroed
        /// </summary>
        /// <returns>True if zeroed</returns>
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed();
        }

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
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
                var attached = await Adjunct.AttachDroneAsync(this, direction).FastPath();

                if (attached)
                {
                    _logger?.Trace($"{nameof(AttachViaAdjunctAsync)}: {direction} attach to adjunct {Adjunct.Description}");
                    _assimulated = true;
                }
                else
                {
                    _logger?.Trace($"{nameof(AttachViaAdjunctAsync)}: [RACE LOST]{direction} attach to adjunct {Adjunct.Description}, {Adjunct.MetaDesc}");
                }

                return attached;
            }
            catch (Exception) when (Zeroed()) { }
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Error(e, $"{nameof(AttachViaAdjunctAsync)}:");                               
            }            
            return false; 
        }

        /// <summary>
        /// Detaches current neighbor
        /// </summary>
        public async ValueTask DropAdjunctAsync()
        {
            var latch = _adjunct;
            if (latch != null && Interlocked.CompareExchange(ref _adjunct, null, latch) == latch)
                await latch.DetachDroneAsync().FastPath();
        }

        /// <summary>
        /// Toggle accounting bit
        /// </summary>
        public void ToggleAccountingBit()
        {
            AccountingBit = !AccountingBit;
        }

        /// <summary>
        /// A test mode
        /// </summary>
        /// 
        private readonly byte[] _vb = new byte[sizeof(ulong)];
        private readonly CcWhisperMsg _m;
        private IoHeap<byte[]> _sendBuf;
        public async ValueTask EmitTestGossipMsgAsync(long v)
        {
            try
            {
                if (Interlocked.Read(ref ((CcCollective) Node).Testing) == 0)
                    return;

                if(!Source.IsOperational() || Adjunct.CcCollective.TotalConnections < 1)
                    return;

                //if (Interlocked.Read(ref _isTesting) > 0)
                //    return;

                //if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                //    return;
            
                //if (Adjunct?.Direction == CcAdjunct.Heading.IsEgress)
                byte[] socketBuf = null;

                try
                {
                    socketBuf = _sendBuf.Take();
                    Write(_vb.AsSpan(), ref v);

                    //var m = new CcWhisperMsg() {Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(vb))};

                    var protoBuf = _m.ToByteArray();
                    var compressed = (ulong)LZ4Codec.Encode(protoBuf, 0, protoBuf.Length, socketBuf, sizeof(ulong), socketBuf.Length - sizeof(ulong));
                    Write(socketBuf, ref compressed);

                    if (!Zeroed())
                    {
                        //Interlocked.Increment(ref AccountingBit);
                        Adjunct.CcCollective.IncEventCounter();

                        var socket = MessageService.IoNetSocket;
                        if (await socket.SendAsync(socketBuf, 0, (int)compressed + sizeof(ulong), timeout: 20).FastPath() > 0)
                        {
                            if (!Adjunct.CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
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
                finally
                {
                    _sendBuf.Return(socketBuf);
                }
            }
            catch (Exception e)
            {
                _logger.Trace(e,Description);
            }
        }

        
    }
}
