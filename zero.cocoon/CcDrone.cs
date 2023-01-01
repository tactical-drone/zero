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
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;
using static System.Runtime.InteropServices.MemoryMarshal;
using Logger = NLog.Logger;

namespace zero.cocoon
{
    public class CcDrone : IoNeighbor<CcProtocMessage<chroniton, CcFrameBatch>>
    {
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="adjunct">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        public CcDrone(IoNode<CcProtocMessage<chroniton, CcFrameBatch>> node, CcAdjunct adjunct,
            IoNetClient<CcProtocMessage<chroniton, CcFrameBatch>> ioNetClient)
            : base
            (
                node,
                ioNetClient,
                static (ioZero, _) => new CcBridgeMessages((CcDrone)ioZero), false
            )
        {
            _logger = LogManager.GetCurrentClassLogger();

            Adjunct = adjunct;

            //Testing
            //var rand = new Random(DateTimeOffset.Now.Ticks.GetHashCode() * DateTimeOffset.Now.Ticks.GetHashCode());

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

            _m = new CcWhisperMsg { Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(_vb)) };

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
                        return _description = $"`drone [{Adjunct?.Hub.Designation.IdString()}, {Adjunct?.Designation.IdString()}], {Adjunct?.Direction},{MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedUtcMs())}'";
                    return _description = $"`drone [{Adjunct?.Hub?.Designation?.IdString()}, {Adjunct?.Designation?.IdString()}], {MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedUtcMs())}'";
                }
                catch
                {
                    return _description = $"`drone [{Adjunct?.Hub?.Designation?.IdString()}, {Adjunct?.Designation?.IdString()}], {MessageService?.IoNetSocket?.LocalAddress} ~> {MessageService?.IoNetSocket?.RemoteAddress}, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedUtcMs())}'";
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
        private CcAdjunct _adjunct;
        /// <summary>
        /// The attached neighbor
        /// </summary>
        public CcAdjunct Adjunct
        {
            get => _adjunct;
            protected internal set => Interlocked.Exchange(ref _adjunct, value);
        }

        public IoNetClient<CcProtocMessage<chroniton, CcFrameBatch>> MessageService => (IoNetClient<CcProtocMessage<chroniton, CcFrameBatch>>)Source;

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

                Volatile.Write(ref _key, $"{Source.Key}`{Adjunct?.Key}");
                return _key;
            }
        }

        /// <summary>
        /// Used for testing
        /// </summary>
        public volatile bool AccountingBit = true;

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
                if ((Adjunct?.WasAttached??false) && UpTime.ElapsedUtcMs() > parm_min_uptime_ms)
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
        public bool AttachViaAdjunct(IIoSource.Heading direction)
        {
            try
            {
                //Raced?
                if (Adjunct.IsDroneAttached || Zeroed())
                    return false;

                //Attach the other way
                var attached = Adjunct.AttachDrone(this, direction);

                _logger?.Trace(attached
                    ? $"{nameof(AttachViaAdjunct)}: {direction} attach to adjunct {Adjunct.Description}"
                    : $"{nameof(AttachViaAdjunct)}: [RACE LOST]{direction} attach to adjunct {Adjunct.Description}, {Adjunct.MetaDesc}");

                return attached;
            }
            catch (Exception) when (Zeroed()) { }
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Error(e, $"{nameof(AttachViaAdjunct)}:");                               
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

                if (!Source.IsOperational() || Adjunct.CcCollective.TotalConnections < 1)
                {
                    _logger.Trace($"{Source.Description}");
                    return;
                }
                    

                //if (Interlocked.Read(ref _isTesting) > 0)
                //    return;

                //if (Interlocked.CompareExchange(ref _isTesting, 1, 0) != 0)
                //    return;
            
                //if (Adjunct?.Direction == CcAdjunct.Heading.IsEgress)
                byte[] buf = null;
                try
                {
                    
                    Write(_vb.AsSpan(), ref v);

                    var protoBuf = _m.ToByteString().Memory;
                    buf = _sendBuf.Take();
                    ulong compressed = (ulong)LZ4Codec.Encode(protoBuf.AsArray(), 0, protoBuf.Length, buf, sizeof(ulong), buf.Length - sizeof(ulong));
                    Write(buf, ref compressed);
                    //Console.WriteLine($"pr->{compressed}({protoBuf.Length}) - {buf.AsSpan().Slice(sizeof(ulong), (int)compressed).ToArray().PayloadSig()}");

                    //buf = _sendBuf.Take(); ;
                    //var bl = new BrotliStream(new MemoryStream(buf), CompressionLevel.Optimal);
                    //bl.BaseStream.Seek(sizeof(ulong), SeekOrigin.Begin);
                    //await bl.WriteAsync(protoBuf.AsArray(), 0, protoBuf.Length);
                    //await bl.FlushAsync();

                    //var tmp = new byte[32];
                    //var tmp2 = new byte[32];
                    //var tmpS = new BrotliStream(new MemoryStream(buf), CompressionMode.Decompress);

                    //tmpS.BaseStream.Seek(sizeof(ulong), SeekOrigin.Begin);
                    //await tmpS.ReadAsync(tmp).FastPath();

                    //Unsafe.As<long[]>(buf)[0] = bl.BaseStream.Position - sizeof(ulong);

                    if (!Zeroed())
                    {
                        //Interlocked.Increment(ref AccountingBit);
                        Adjunct.CcCollective.IncEventCounter();

                        var socket = MessageService.IoNetSocket;
                        var sent = 0;
                        _logger.Trace($"{nameof(EmitTestGossipMsgAsync)}: hup sending {(int)compressed + sizeof(ulong)} bytes to {socket.RemoteAddress}...");
                        if ((sent = await socket.SendAsync(buf, 0, (int)compressed + sizeof(ulong), timeout: 20).FastPath()) > 0) 
                        //if (await socket.SendAsync(buf, 0, (int)bl.BaseStream.Position, timeout: 20).FastPath() > 0)
                        {
                            _logger.Trace($"{nameof(EmitTestGossipMsgAsync)}: hup sent {sent} bytes to {socket.RemoteAddress}; [SUCCESS]");
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
                    _sendBuf.Return(buf);
                }
            }
            catch (Exception e)
            {
                _logger.Trace(e,Description);
            }
        }


    }
}
