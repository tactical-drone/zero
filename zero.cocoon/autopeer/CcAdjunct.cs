//#define LOSS
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
#if DEBUG
using System.Text;
#endif
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using K4os.Compression.LZ4;
using MathNet.Numerics.Random;
using NLog;
using Org.BouncyCastle.Bcpg;
using Org.BouncyCastle.Crypto;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.core.conf;
using zero.core.core;
using zero.core.feat.misc;
using zero.core.feat.models.protobuffer;
using zero.core.feat.patterns.time;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;
using Zero.Models.Protobuf;
using Logger = NLog.Logger;

namespace zero.cocoon.autopeer
{
    /// <summary>
    /// Processes (UDP) discovery messages from the collective.
    /// </summary>
    public class CcAdjunct : IoNeighbor<CcProtocMessage<chroniton, CcDiscoveryBatch>>
    {
        public CcAdjunct(CcHub node, IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>> ioNetClient, object extraData = null)
            : base
            (
                node,
                ioNetClient,
                static (ioZero, _) => new CcDiscoveries((CcAdjunct)ioZero, "discovery msg", groupByEp: false),
                extraData == null, false, concurrencyLevel:ioNetClient.ZeroConcurrencyLevel
            )
        {
            //parm_io_batch_size = CcCollective.parm_max_adjunct * 2;

            if (Source.Zeroed())
                return;

            _random = new CryptoRandomSource(true);

            _logger = LogManager.GetCurrentClassLogger();

            //TODO tuning
            var capMult = CcCollective.ZeroDrone ? 10 : 7;
            var capBase = 2;

            _probeRequest = new IoZeroMatcher($"{nameof(_probeRequest)}, proxy = {IsProxy}", CcCollective.MaxAdjuncts>>3, parm_max_network_latency_ms << 1, (int)Math.Pow(capBase, capMult), true);
            _fuseRequest = new IoZeroMatcher($"{nameof(_fuseRequest)}, proxy = {IsProxy}", CcCollective.MaxAdjuncts>>3, parm_max_network_latency_ms << 1, (int)Math.Pow(capBase, capMult), true);
            _scanRequest = new IoZeroMatcher($"{nameof(_scanRequest)}, proxy = {IsProxy}", CcCollective.MaxAdjuncts>>3, parm_max_network_latency_ms << 1, (int)Math.Pow(capBase, capMult), true);

            if (extraData != null)
            {
                var (ccDesignation, ipEndPoint, verified) = (ValueTuple<CcDesignation, IPEndPoint, bool>) extraData;

                if (Equals(ipEndPoint.Address, IPAddress.Any))
                    throw new ArgumentException($"Invalid destination IP address specified: {ipEndPoint.Address}");

                IsProxy = true;
                
                Designation = ccDesignation;
                Key = $"udp://{ipEndPoint}`{Designation.IdString()}";

                _address = IoNodeAddress.CreateFromEndpoint("udp", ipEndPoint);

                //to prevent cascading into the hub we clone the source.
                Source = new IoUdpClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>($"UDP Proxy ~> {base.Description}", MessageService, _address.IpEndPoint, verified? IoSocket.Connection.Egress:IoSocket.Connection.Ingress);
                
                IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                {
                    if(!((IoNanoprobe)state).Zeroed())
                        await ((CcAdjunct)state).Source.ZeroHiveAsync((CcAdjunct)state).FastPath();
                }, this);
                
                CompareAndEnterState(verified ? AdjunctState.Verified : AdjunctState.Unverified, AdjunctState.Undefined);
            }
            else
            {
                _v = new ConcurrentDictionary<string, long>();
                Designation = CcCollective.CcId;
                
                //Services = services ?? node.Services;
                Key = $"{Source.Key}`{Designation.IdString()}";

                CompareAndEnterState(AdjunctState.Local, AdjunctState.Undefined);
                _routingTable = new ConcurrentDictionary<string, CcAdjunct>();

                _address = Hub.Address;
            }

#if DEBUG
            string packetHeapDesc = $"{nameof(_chronitonHeap)}, {_description}";
            string protoHeapDesc = $"{nameof(_sendBuf)}, {_description}";
#else
            string packetHeapDesc = string.Empty;
            string protoHeapDesc = string.Empty;
#endif

            //TODO tuning:
            _chronitonHeap = new IoHeap<chroniton, CcAdjunct>(packetHeapDesc, CcCollective.ZeroDrone & !IsProxy ? 32 : 16,
                static (_, @this) => new chroniton
                {
                    PublicKey = UnsafeByteOperations.UnsafeWrap(@this.CcCollective.CcId.PublicKey)
                }, true, this);

            _sendBuf = new IoHeap<Tuple<byte[],byte[]>>(protoHeapDesc, CcCollective.ZeroDrone & !IsProxy ? 32 : 16, static (_, _) => Tuple.Create(new byte[1492 / 2], new byte[1492 / 2]), autoScale: true);

            //var nrOfStates = Enum.GetNames(typeof(AdjunctState)).Length;

            Interlocked.Exchange(ref _scanAge, Environment.TickCount - (int)TimeSpan.FromHours(1).TotalMilliseconds);
            Interlocked.Exchange(ref _lastSeduced, Environment.TickCount - parm_max_network_latency_ms * 2);

            //if (CcCollective.ZeroDrone)
            //    Volatile.Write(ref ZeroRoot, ZeroSyncRoot(concurrencyLevel: Source.ZeroConcurrencyLevel * 20 + 200, AsyncTasks));
            //else
            //    Volatile.Write(ref ZeroRoot, ZeroSyncRoot(concurrencyLevel: (int)(Source.ZeroConcurrencyLevel * parm_max_swept_drones), AsyncTasks));
        }

        public enum AdjunctState
        {
            Undefined = 0,
            FinalState = 1,
            Local = 2,
            Unverified = 3,
            Verified = 4,
            Fusing = 5,
            Connecting = 6,
            Connected = 7
        }

        /// <summary>
        /// The current state
        /// </summary>
        private
        #if RELEASE 
        readonly
        #endif
        IoStateTransition<AdjunctState> _state = new()
        {
            FinalState = AdjunctState.FinalState
        };

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;


        private string _description;

        public override string Description
        {
            get
            {
                try
                {
                    if (Zeroed())
                        return _description;

                    if (IsProxy)
                        return _description = $"adjunct[{CcCollective.EgressCount},{CcCollective.IngressCount}] {MessageService.IoNetSocket.Kind} [{Serial}]({EventCount}, {_drone?.EventCount ?? 0}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({ConnectionAttempts}:{OpenSlots}]; {MessageService.IoNetSocket.LocalAddress}, {MessageService.IoNetSocket.RemoteAddress}, [{Hub?.Designation.IdString()}, {Designation.IdString()}]'";
                    
                    return _description = $"hub[{CcCollective?.EgressCount},{CcCollective?.IngressCount}] [{Serial}]({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({ConnectionAttempts}:{OpenSlots}]; local = {MessageService.IoNetSocket.LocalAddress} ~> {MessageService.IoNetSocket.RemoteAddress}|, [{Hub?.Designation?.IdString()}, {Designation.IdString()}]'";
                }
                catch (Exception)
                {
                    if(IsProxy)
                        return _description?? $"adjunct({EventCount}, {_drone?.EventCount ?? 0}, , {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({OpenSlots}):{ConnectionAttempts}:{OpenSlots}]; l = {MessageService?.IoNetSocket?.LocalAddress}, r ={MessageService?.IoNetSocket?.RemoteAddress},' [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], T = {UpTime.ElapsedUtcMsToSec() / 3600:0.00}h'";    
                    else
                        return _description = $"hub ({EventCount}, {(WasAttached ? "C!" : "dc")})[{TotalPats}~{SecondsSincePat:000}s:P({ConnectionAttempts}:{OpenSlots}]; l = {MessageService?.IoNetSocket?.LocalAddress}, r ={MessageService?.IoNetSocket?.RemoteAddress}, [{Hub?.Designation?.IdString()}, {Designation?.IdString()}], T = {UpTime.ElapsedUtcMsToSec() / 3600:0.00}h'";
                }
            }
        }

        /// <summary>
        /// return extra information about the state
        /// </summary>
#if DEBUG
        public string MetaDesc => $"{(Zeroed() ? "Zeroed!!!," : "")} d={Direction},s={State},v={(Verified?"1":"0")},a={(Assimilating ? "1":"0")},da={(IsDroneAttached ? "1":"0")},cc={(IsDroneConnected ? "1":"0")},g={(IsGossiping ? "1":"0")},arb={(IsArbitrating ? "1":"0")}";
#else
        public string MetaDesc => string.Empty;
#endif

        private Task BackOffAsync => Task.Delay(RandomNumberGenerator.GetInt32(parm_max_network_latency_ms >> 3, parm_max_network_latency_ms), AsyncTasks.Token);
        private Task BackOffAsyncSmall => Task.Delay(RandomNumberGenerator.GetInt32(parm_max_network_latency_ms >> 5, parm_max_network_latency_ms>>4), AsyncTasks.Token);

        private bool StochasticRate => RandomNumberGenerator.GetInt32(0, 2) == 0;

        /// <summary>
        /// Random number generator
        /// </summary>
        private readonly Random _random;

        /// <summary>
        /// Discovery services
        /// </summary>
        protected internal CcHub Hub => (CcHub)Node;

        /// <summary>
        /// Source
        /// </summary>
        public IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>> MessageService => (IoNetClient<CcProtocMessage<chroniton, CcDiscoveryBatch>>) Source;


        /// <summary>
        /// Holds all messages
        /// </summary>
        private IoHeap<chroniton, CcAdjunct> _chronitonHeap;

        /// <summary>
        /// Holds all coded output streams used for sending messages
        /// </summary>
        private readonly IoHeap<Tuple<byte[],byte[]>> _sendBuf;

        /// <summary>
        /// The udp routing table 
        /// </summary>
        private ConcurrentDictionary<string, CcAdjunct> _routingTable;

        /// <summary>
        /// The gossip drone associated with this neighbor
        /// </summary>
        public CcDrone Drone => _drone;
        private CcDrone _drone;
        

        /// <summary>
        /// Whether The drone is attached
        /// </summary>
        public bool IsDroneAttached => _drone != null && !_drone.Zeroed();

        /// <summary>
        /// Whether the drone is nominal
        /// </summary>
        public bool IsDroneConnected => IsDroneAttached && State == AdjunctState.Connected && Direction != IIoSource.Heading.Undefined;
        
        /// <summary>
        /// If the adjunct is working 
        /// </summary>
        public bool Assimilating => !Zeroed() && Verified;

        /// <summary>
        /// Whether the node, drone and adjunct are nominal
        /// </summary>
        public bool IsGossiping => IsDroneConnected && Assimilating;

        /// <summary>
        /// Indicates whether we have extracted information from this drone
        /// </summary>
        public bool Assimilated { get; protected set; }

        private int _openSlots;
        /// <summary>
        /// The number of fuse requests received
        /// </summary>
        public int OpenSlots => _openSlots;

        private int _connectionAttempts;
        /// <summary>
        /// Number of fuse requests sent
        /// </summary>
        public int ConnectionAttempts => _connectionAttempts;

        private int _zeroProbes;
        /// <summary>
        /// Number of successful probes
        /// </summary>
        public int ZeroProbes => _zeroProbes;

        private int _scanCount;
        public int ScanCount => Volatile.Read(ref _scanCount);

        /// <summary>
        /// Gathers counter-intelligence at acceptable rates
        /// </summary>
        private int _lastSeduced;
        private int _seduceBurst;
        //uptime
        private long _attachTimestamp;
        public long AttachTimestamp
        {
            get => Interlocked.Read(ref _attachTimestamp);
            private set => Interlocked.Exchange(ref _attachTimestamp, value);
        }

        private readonly IoNodeAddress _address;
        /// <summary>
        /// The adjunct address
        /// </summary>
        public IoNodeAddress Address => _address;

        /// <summary>
        /// The adjunct address
        /// </summary>
        public IPEndPoint Session { get; protected set; }

        /// <summary>
        /// The adjunct address
        /// </summary>
        public IPEndPoint Dmz { get; protected set; }

        /// <summary>
        /// Whether this adjunct contains verified remote client connection information
        /// </summary>
        public bool IsProxy { get; }

        /// <summary>
        /// The node identity
        /// </summary>
        public CcDesignation Designation { get; protected set; }

        /// <summary>
        /// Whether the node has been verified
        /// </summary>
        public bool Verified => State > AdjunctState.Unverified;

        /// <summary>
        /// Backing cache
        /// </summary>
        private int _direction;

        /// <summary>
        /// Who contacted who?
        /// </summary>
        //public Kind Direction { get; protected set; } = Kind.Undefined;
        public IIoSource.Heading Direction => (IIoSource.Heading) Volatile.Read(ref _direction);

        /// <summary>
        /// inbound
        /// </summary>
        public bool IsIngress =>  IsDroneConnected && Direction == IIoSource.Heading.Ingress;

        /// <summary>
        /// outbound
        /// </summary>
        public bool IsEgress => IsDroneConnected && Direction == IIoSource.Heading.Egress;

        /// <summary>
        /// The node that this adjunct belongs to
        /// </summary>
        public CcCollective CcCollective => Hub.CcCollective;

        /// <summary>
        /// The router
        /// </summary>
        public CcAdjunct Router => Hub.Router;

        /// <summary>
        /// Receives protocol messages from here
        /// </summary>
        private IoConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> _protocolConduit;

        
        /// <summary>
        /// Time since pat
        /// </summary>
        private int _lastPat = Environment.TickCount - (int)TimeSpan.FromHours(1).TotalMilliseconds;

        /// <summary>
        /// Total pats
        /// </summary>
        private long _totalPats;

        /// <summary>
        /// Seconds since last adjunct pat
        /// </summary>
        protected int LastPat
        {
            get => _lastPat;
            // ReSharper disable once ValueParameterNotUsed
            set
            {
                Interlocked.Exchange(ref _lastPat, value);
                Interlocked.Increment(ref _totalPats);
            }
        }

        /// <summary>
        /// Seconds since last adjunct pat
        /// </summary>
        public long TotalPats => Interlocked.Read(ref _totalPats);

        /// <summary>
        /// Seconds since valid
        /// </summary>
        public long SecondsSincePat => LastPat.ElapsedMsToSec() - 1;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _scanRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _fuseRequest;

        /// <summary>
        /// Used to Match requests
        /// </summary>
        private IoZeroMatcher _probeRequest;

        /// <summary>
        /// Whether this adjunct exists in the Hub's routing table
        /// </summary>
        private int _probed = 0;

        /// <summary>
        /// Whether we are probed
        /// </summary>
        public bool Probed => _probed > 0;

        /// <summary>
        /// The CcId
        /// </summary>
        public override string Key { get; }

        /// <summary>
        /// Whether to enable syn triggers. Useful for seduction, 
        /// </summary>
        private const bool EnableSynTrigger = true;

        
        private int _scanAge;
        /// <summary>
        /// Time since our last scan
        /// </summary>
        public long ScanAge => Volatile.Read(ref _scanAge);

        /// <summary>
        /// V
        /// </summary>
        private readonly ConcurrentDictionary<string, long> _v;

        /// <summary>
        /// V
        /// </summary>
        public ConcurrentDictionary<string, long> V => _v;

        /// <summary>
        /// V flush timestamp
        /// </summary>
        private int _vFlush = Environment.TickCount;

        public long Lamport => _lamport;
        private long _lamport;
        public bool LivenessTest
        {
            get
            {
                var c = CcCollective.Lamport;
                return Math.Abs(_lamport - c) >= c / 2;
            }
        }

        private bool _retryOnDc = true;
        public bool RetryOnDc => _retryOnDc;

        /// <summary>
        /// The adjunct services
        /// </summary>
        //public CcService Services { get; protected set; }

        /// <summary>
        /// Salt length
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_length = 20;

        /// <summary>
        /// Salt time to live
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_salt_ttl = 2 * 60 * 60;

        /// <summary>
        /// Max expected network delay
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
#if DEBUG
        public int parm_max_network_latency_ms = 2000;
#else
        public int parm_max_network_latency_ms = 1000;
#endif

        /// <summary>
        /// Time allowed before webbing
        /// </summary>
        [IoParameter]
        public int parm_web_settle_ms = 250;

        /// <summary>
        /// Rate limit DMZ probe attempts to one per time unit
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_v_heuristic_ms = 1000;

        /// <summary>
        /// Rate limit DMZ probe burst rate per time unit <see cref="parm_max_v_heuristic_ms"/>
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_v_rate = 3;

        /// <summary>
        /// Maximum number of drones in discovery response
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_swept_drones = 6;

        /// <summary>
        /// Maximum number of services supported
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_services = 3;
        
        /// <summary>
        /// Maximum connection retry on non responsive nodes 
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_zombie_max_connection_attempts = 5;

        /// <summary>
        /// Minimum pats before a node could be culled
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_pats_before_shuffle = 0;

        /// <summary>
        /// Minimum number of desired spare bays for sweep requests to yield returns.
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_spare_bays = 1;

        /// <summary>
        /// Network id
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_network_id = 13;

        /// <summary>
        /// Protocol version
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_protocol_version = 0;

        /// <summary>
        /// Protocol version
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_seduce_burst = 2; //2 of each

        /// <summary>
        /// Zeroed?
        /// </summary>
        /// <returns>True if zeroed</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            try
            {
                return base.Zeroed() || (_protocolConduit?.Zeroed()??false) || Source.Zeroed() || Hub.Zeroed() || CcCollective.Zeroed();
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            
#if SAFE_RELEASE
            _routingTable = null;
            _probeRequest = null;
            _fuseRequest = null;
            _scanRequest = null;
            _drone = null;
            _protocolConduit = null;
            _chronitonHeap = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            try
            {
                await base.ZeroManagedAsync().FastPath();
            }
            catch (Exception e)
            {
                _logger.Error(e);
            }

            ResetState(AdjunctState.FinalState);

            if (IsProxy)
            {
                try
                {
                    //Remove from routing table
                    if (Router is { _routingTable: { } } && Router._routingTable.TryGetValue(Address.IpPort, out var currentRoute))
                    {
                        if (currentRoute == this)
                        {
                            if (!Router._routingTable.TryRemove(Address.IpPort, out _) && Verified)
                            {
                                _logger.Warn($"Failed to remove route {Address.IpPort}, {Description}");
#if DEBUG
                                foreach (var route in Router._routingTable)
                                {
                                    _logger.Trace($"R/> {nameof(Router._routingTable)}[{route.Key}]=<{route.Value.Address.Port}> {route.Value}");
                                }
#endif
                            }
                        }
#if TRACE
                        else
                        {
                            _logger.Trace($"ROUTER: cull ignored,wanted = [{Designation?.IdString()}], got [{currentRoute.Designation.IdString()}, serial1 = {Serial} vs {currentRoute.Serial}], {Description}");
                        }
#endif
                    }

                    Router.V.TryRemove(Address.IpPort, out _);
                }
                catch
                {
                    // ignored
                }
                await DetachDroneAsync().FastPath();

                //Emit event
                try
                {
                    if (!CcCollective.ZeroDrone && _eventStreamAdded && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.RemoveAdjunct,
                            Adjunct = new Adjunct
                            {
                                Id = Designation!.IdString(),
                                CollectiveId = CcCollective.CcId.IdString()
                            }
                        });
                }
                catch
                {
                    // ignored
                }
            }
            else
            {
                //_logger.Info($"ZERO HUB PROXY: from: {ZeroedFrom?.Description}, reason: {ZeroReason}, {Description}");
            }

            await _chronitonHeap.ZeroManagedAsync(static (item, @this) =>
            {
                ArrayPool<byte>.Shared.Return(item.Signature.Memory.AsArray());
                return new ValueTask(Task.CompletedTask);
            }, this);

            await _sendBuf.ZeroManagedAsync<object>().FastPath();

            if (!CcCollective.ZeroDrone && Assimilated && WasAttached && UpTime.ElapsedUtcMs() > parm_min_uptime_ms && EventCount > 10)
                _logger.Debug($"- {(Assimilated ? "apex" : "sub")} {Description}; reason = {ZeroReason}");

            await _probeRequest.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            await _fuseRequest.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            await _scanRequest.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

#if TRACE
            _logger.Trace($"Closed {ZeroReason}");
#endif
        }

        /// <summary>
        /// Robotech
        /// </summary>
        /// <param name="this"></param>
        /// <returns></returns>
        private static async ValueTask RoboAsync(CcAdjunct @this)
        {
            try
            {
                //TODO: tuning, helps cluster test bootups not stalling on popdog spam
                
                var ioTimer = new IoTimer(TimeSpan.FromSeconds(@this.CcCollective.parm_mean_pat_delay_s>>4), @this.AsyncTasks.Token);
                var ts = Environment.TickCount;
                while (!@this.Zeroed())
                {
                    _ = await ioTimer.TickAsync().FastPath();
                    if (@this.Zeroed())
                        break;

                    var targetDelay = (@this.CcCollective.TotalConnections < @this.CcCollective.parm_max_outbound ? @this.CcCollective.parm_mean_pat_delay_s>>2 : @this.CcCollective.parm_mean_pat_delay_s) * 1000;

                    if (ts.ElapsedMs() < targetDelay)
                        continue;

                    
#if TRACE
                    @this._logger.Trace($"Robo - {TimeSpan.FromMilliseconds(d)}, {@this.Description}");
#endif

                    try
                    {
#if DEBUG
                        if (ts.ElapsedMs() > targetDelay + @this.parm_error_popdog && ts.ElapsedMs() > 0)
                        {
                            @this._logger.Warn(
                                $"{@this.Description}: Popdog is slow!!!, {(ts.ElapsedMs() - targetDelay) / 1000.0:0.0}s");
                        }

                        if (ts.ElapsedMs() < targetDelay - @this.parm_error_popdog && !@this.Zeroed())
                        {
                            @this._logger.Warn(
                                $"{@this.Description}: Popdog is FAST!!!, {(ts.ElapsedMs() - targetDelay):0.0}ms / {targetDelay}");
                        }
#endif

                        foreach (var ioNeighbor in @this.Hub.Neighbors.Values.Where(n => ((CcAdjunct)n).IsProxy))
                        {
                            var adjunct = (CcAdjunct)ioNeighbor;
                            //if (!adjunct.IsDroneConnected)
                                await adjunct.EnsureRoboticsAsync().FastPath();
                        }
                    }
                    catch (Exception) when (@this.Zeroed())
                    {
                    }
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger.Fatal(e, $"{@this.Description}: Watchdog returned with errors!");
                    }
                    finally
                    {
                        ts = Environment.TickCount;
                    }
                }
            }
            finally
            {
                if (!@this.Zeroed())
                {
                    @this._logger.Fatal($"{@this.Description}: WATCHDOG died!!!");
                }
            }
        }

        /// <summary>
        /// Ensure robotic diagnostics
        /// </summary>
        public async ValueTask EnsureRoboticsAsync()
        {
            try
            {
                if (SecondsSincePat >= CcCollective.parm_mean_pat_delay_s >> 2)
                {
                    //send PAT
                    if (!await ProbeAsync("SYN-PAT").FastPath())
                    {
                        if (!Zeroed())
                            _logger.Error($"-/> {nameof(ProbeAsync)}: SYN-PAT [FAILED], {Description}, {MetaDesc}");
                    }
#if TRACE
                    else 
                        _logger.Trace($"-/> {nameof(ProbeAsync)}: SYN-PAT [SUCCESS], {Description}");
#endif
                }

                //Watchdog failure
                if (SecondsSincePat >= CcCollective.parm_mean_pat_delay_s << 1)
                {
                    _logger.Trace($"w {nameof(EnsureRoboticsAsync)} - {Description}, s = {SecondsSincePat} >> {CcCollective.parm_mean_pat_delay_s}, {MetaDesc}");
                    await DisposeAsync(CcCollective,$"-wd: l = {SecondsSincePat}s, up = {TimeSpan.FromMilliseconds(UpTime.ElapsedUtcMs()).TotalHours:0.00}h").FastPath();
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(EnsureRoboticsAsync)} - {Description}: Send PAT failed!");
            }
        }

        private delegate ValueTask BlockOnReplicateAsyncDelegate();

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <returns></returns>
        public override async ValueTask BlockOnReplicateAsync()
        {
            try
            {
                if (!IsProxy)
                {
                    //Discoveries
                    //Start processing on adjunct
                    await ZeroAsync(static async @this =>
                    {
                        await @this.ProcessDiscoveriesAsync().FastPath();
                    }, this).FastPath();

                    //Watchdog
                    await ZeroAsync(RoboAsync, this).FastPath();
                    
                    //base
                    await base.BlockOnReplicateAsync().FastPath();
                }
                else
                {
                    //emit event
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                    {
                        _eventStreamAdded = true;
                        Interlocked.MemoryBarrier();
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.AddAdjunct,
                            Adjunct = new Adjunct
                            {
                                Id = Designation.IdString(),
                                CollectiveId = Router.Designation.IdString(),
                                Ip = Address.Ip,
                                AnimatorPort =  AutoPeeringEventService.Port == 27021? Address.Port + 1: 27021,
                            }
                        });
                    }
                }
            }
            catch when(Zeroed()){}
            catch(Exception e)when (!Zeroed())
            {
                _logger.Error(e, $"{Description}: {nameof(BlockOnReplicateAsync)} Failed!");
            }
        }

        /// <summary>
        /// Connect to an adjunct, turning it into a drone for use
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        protected async ValueTask<bool> ConnectAsync(IPEndPoint dest)
        {
            try
            {
                //Validate request
                if (!Assimilating && !IsDroneConnected)
                {
                    _logger.Fatal($"{Description}: Connect aborted, wrong state: {MetaDesc}");
                    PrintStateHistory();
                    return false;
                }

                if (!Hub.ContainsId(Designation.IdString()) && CcCollective.Hub.Neighbors.TryGetValue(Key, out var existingNeighbor))
                {
                    try
                    {
                        if (existingNeighbor?.Source?.IsOperational()??false)
                        {
                            _logger.Trace($"Drone already operational, dropping {existingNeighbor.Description}");
                            return false;
                        }
                        if(!existingNeighbor?.Zeroed()??false)
                            await existingNeighbor.DisposeAsync(this, $"Dropped because stale from {existingNeighbor.Description}").FastPath();
                    }
                    catch when (existingNeighbor?.Source?.Zeroed()?? true) {}
                    catch (Exception e)when(!existingNeighbor?.Source?.Zeroed()??false)
                    {
                        _logger.Error(e,$"{nameof(ConnectAsync)}:");
                    }
                }

                IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                {
                    var (@this,dest) = (ValueTuple<CcAdjunct, IPEndPoint>)state;
                    try
                    {
                        var ts = Environment.TickCount;
                        AdjunctState oldState;
                        
                        if ((@this.State != AdjunctState.Connecting || @this.CurrentState.EnterTime.ElapsedMs() <= @this.parm_max_network_latency_ms) &&
                            (oldState = @this.CompareAndEnterState(AdjunctState.Connecting, AdjunctState.Fusing)) != AdjunctState.Fusing)
                        {
                            @this._logger.Trace($"{nameof(ConnectAsync)} - {@this.Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Fusing)}");
                            return;
                        }
                        
                        //Attempt the connection, race to win
                        if (!await @this.CcCollective.ConnectToDroneAsync(@this, dest).FastPath())
                        {
                            @this.CompareAndEnterState(AdjunctState.Verified, AdjunctState.Connecting);
                            @this._logger.Trace($"{@this.Description}: Leashing adjunct failed!; state = {@this.State}");

                            //if(@this.StochasticRate)  
                            //    await @this.SeduceAsync("SYN-DENIED", IIoSource.Heading.Ingress).FastPath();
                            
                        }
                        else //Track some connection perf stats
                        {
                            Interlocked.Add(ref ConnectionTime, ts.ElapsedMs());
                            Interlocked.Increment(ref ConnectionCount);
                        }
                    }
                    catch when (@this.Zeroed()){}
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger.Trace(e);
                    }
                }, (this,dest));

                return true;
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Error(e, $"{nameof(ConnectAsync)}:");
            }

            return false;
        }

        /// <summary>
        /// Unpacks a batch of messages
        /// 
        /// Batching strategies needs to be carefully matched with the nature of incoming traffic, or bad things will happen.
        /// </summary>
        /// <param name="batchJob">The consumer that need processing</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="nanite"></param>
        /// <returns>Task</returns>
        private async ValueTask ZeroUnBatchAsync<T>(IoSink<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> batchJob,
            Func<CcBatchMessage, T, CcAdjunct, ValueTask> processCallback, T nanite)
        {
            if (batchJob == null)
                return;

            CcDiscoveryBatch msgBatch = default;
            try
            {
                var job = (CcProtocBatchJob<chroniton, CcDiscoveryBatch>)batchJob;
                msgBatch = job.Get();

                if(msgBatch == null)
                    return;

                //Grouped by ingress endpoint batches (this needs to make sense or disable)
                if (msgBatch.GroupByEpEnabled)
                {
                    foreach (var epGroups in msgBatch.GroupBy)
                    {
                        if (Zeroed())
                            break;
                        if (msgBatch.GroupBy.TryGetValue(epGroups.Key, out var msgGroup))
                        {
                            if(!msgGroup.Item2.Any())
                                continue;

                            var srcEndPoint = epGroups.Value.Item1.GetEndpoint();
                            var proxy = await RouteAsync(srcEndPoint, epGroups.Value.Item2[0].Zero.PublicKey).FastPath();
                            if (proxy != null)
                            {
                                foreach (var message in msgGroup.Item2)
                                {
                                    //if (message.SourceState > 0)
                                    //{
                                    //    var routed = Router._routingTable.TryGetValue(message.EndPoint.GetEndpoint().ToString(), out var zombie);
                                    //    if (routed)
                                    //    {
                                    //        await zombie.DisposeAsync(this, "Connection RESET!!!");
                                    //        break;
                                    //    }
                                    //    continue;
                                    //}

                                    IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                                    {
                                        var (processCallback, message, nanite, proxy) = (ValueTuple<Func<CcBatchMessage, T, CcAdjunct, ValueTask>, CcBatchMessage, T, CcAdjunct>)state;
                                        await processCallback(message, nanite, proxy).FastPath();
                                    }, (processCallback, message, nanite, proxy));
                                }
                            }
                        }
                        msgBatch.GroupBy.Clear();
                    }
                }
                else //ZeroDefault and safe mode
                {
                    CcAdjunct proxy = null;

                    if (msgBatch.Count > msgBatch.Capacity * 2 / 3)
                        _logger.Warn($"{nameof(ZeroUnBatchAsync)}: -> large batches detected; size = {msgBatch.Count}/{msgBatch.Capacity}");

                    for (var i = 0; i < msgBatch.Count; i++)
                    {
                        if (Zeroed())
                            break;

                        var message = msgBatch.Messages[i];

                        Debug.Assert(message.Zero != null);
                        try
                        {
                            //TODO, is this caching a good idea? 
                            if (proxy is not { IsProxy: true })
                            {
                                //if (message.SourceState > 0)
                                //{
                                //    var routed = Router._routingTable.TryGetValue(cachedEp.GetEndpoint().ToString(),
                                //        out var zombie);
                                //    if (routed)
                                //    {
                                //        await zombie.DisposeAsync(this, "Connection RESET!!!");
                                //        break;
                                //    }

                                //    continue;
                                //}

                                proxy = await RouteAsync(message.EndPoint.GetEndpoint(), message.Zero.PublicKey).FastPath();

                                if (proxy == null)
                                    continue;

                                //if (message.SourceState > 0)
                                //{
                                //    await proxy.DisposeAsync(this, "Connection has been reset!!!").FastPath();
                                //    break;
                                //}
                            }

                            //if (Equals(message.Zero.Header.Ip.Dst.GetEndpoint(),
                            //        Router.MessageService.IoNetSocket.NativeSocket.LocalEndPoint))
                            {
                                //IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                                //{
                                   // var (processCallback, message, nanite, proxy) = (ValueTuple<Func<CcBatchMessage, T, CcAdjunct, ValueTask>, CcBatchMessage, T, CcAdjunct>)state;
                                   IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                                   {
                                       var (processCallback, message, nanite, proxy) = (ValueTuple<Func<CcBatchMessage, T, CcAdjunct, ValueTask>, CcBatchMessage, T, CcAdjunct>)state;
                                       await processCallback(message, nanite, proxy).FastPath();
                                   },(processCallback, message,nanite,proxy));

                                //}, (processCallback, message, nanite, proxy));
                            }
                        }
                        catch (Exception) when (Zeroed()) { }
                        catch (Exception e) when (!Zeroed())
                        {
                            _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                        }
                    }
                }

                await batchJob.SetStateAsync(IoJobMeta.JobState.Consumed).FastPath();
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Debug(e, "Error processing message batch");
            }
            finally
            {
                try
                {
                    msgBatch!.ReturnToHeap();
                }
                catch
                {
                    // ignored
                }
            }
        }

        /// <summary>
        /// Routes Messages according to src IP
        /// </summary>
        /// <param name="srcEndPoint">The source of the request</param>
        /// <param name="publicKey">The public key of the request</param>
        /// <param name="alternate">for debug purposes</param>
        /// <returns>The proxy</returns>
        private async ValueTask<CcAdjunct> RouteAsync(IPEndPoint srcEndPoint, ByteString publicKey, IPEndPoint alternate = null)
        {
            var key = srcEndPoint.ToString();
            var routed = Router._routingTable.TryGetValue(key, out var proxy);
            //Router
            try
            {
                if (!routed)
                {
                    //Console.WriteLine($"---- {key} is not Routed!!!");
                    if (Node == null)
                        return null;

                    try
                    {
                        proxy = (CcAdjunct)Hub.Neighbors.Values.First(n => ((CcAdjunct)n).IsProxy && Equals(((CcAdjunct)n).Address.IpEndPoint, srcEndPoint) && ((CcAdjunct)n).Designation.PublicKey.ArrayEqual(publicKey.Memory));
                    }
                    catch { }
                    
                    if (proxy != null && !proxy.Zeroed())
                    {
                        //TODO, proxy adjuncts need to malloc the same way when listeners spawn them.
                        if (!Router._routingTable.TryAdd(key, proxy))
                        {
                            Router._routingTable.TryGetValue(key, out proxy);
                        }
                        else
                        {
                            _logger.Trace($"Adjunct [ROUTED]: {proxy.MessageService.IoNetSocket.Kind} {MessageService.IoNetSocket.LocalAddress} ~> {srcEndPoint}");
                        }
                    }
                }
                //verify pk
                else if (proxy is { IsProxy: true } && !proxy.Designation.PublicKey.ArrayEqual(publicKey.Memory))
                {
                    var pk1 = proxy.Designation.IdString();
                    var pk2 = CcDesignation.MakeKey(publicKey);
                    var msg = $"{nameof(Router)}: adjunct rejected; [BAD SIGNATURE or PROXY]; {proxy.Address}/{pk1}, key = {key}/{pk2}: state = {proxy.State}, up = {proxy.UpTime.ElapsedUtcMsToSec()}s, events = {proxy.EventCount}, | {proxy.Description}";

                    if (!proxy.IsDroneConnected || proxy is { State: < AdjunctState.Verified }) 
                    {
                        _logger.Fatal($"{nameof(RouteAsync)}: stale route {proxy.Description}");
                        await proxy.DisposeAsync(this, msg).FastPath();
                        await Task.Delay(1); //TODO:??
                    }

                    _logger.Warn(msg);
                    return Router;
                }

                //Console.WriteLine($"---- {key} is Routed, {Router.Description}");
            }
            catch when (Zeroed() || (proxy?.Zeroed() ?? false)) { }
            catch (Exception e) when (!Zeroed() && !(proxy?.Zeroed() ?? false))
            {
                _logger.Error(e, $"{nameof(Router)} failed with errors: ");
                return null;
            }

            if (proxy != null)
            {
#if DEBUG
                if (proxy.Designation.IdString() != CcDesignation.MakeKey(publicKey))
                {
                    if (alternate != null)
                    {
                        //var badProxy = proxy;
                        proxy = await RouteAsync(alternate, publicKey).FastPath();
                        if (proxy.IsProxy && proxy.Designation.IdString() != CcDesignation.MakeKey(publicKey))
                        {
                            _logger!.Warn($"Invalid routing detected wanted = {proxy.Designation.IdString()} - {proxy.Address.IpEndPoint}, got = {CcDesignation.MakeKey(publicKey)} - {srcEndPoint}");
                            proxy = Router;
                        }
                    }
                }
                else
#endif
                {
                    IncEventCounter();
                    proxy.IncEventCounter();
                }
            }
            else
            {
                proxy = Router;
            }

            return proxy;
        }

        private const PaddingMode PaddingMode = System.Security.Cryptography.PaddingMode.PKCS7;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static byte[] AesEncryptToBytes(ReadOnlySpan<byte> buffer, byte[] key, CcDesignation d)
        {
            using var aesAlg = Aes.Create();
            aesAlg.Key = key;
            aesAlg.IV = d.Iv;
            aesAlg.Padding = PaddingMode;

            using (var msEncrypt = new MemoryStream())
            {
                using (var csEncrypt = new CryptoStream(msEncrypt, aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV), CryptoStreamMode.Write))
                {
                    using (var swEncrypt = new BinaryWriter(csEncrypt))
                    {
                        swEncrypt.Write(buffer);
                    }
                    return msEncrypt.ToArray();
                }
            }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static Memory<byte> AesDecryptFromBytes(ReadOnlyMemory<byte> cipherText, byte[] key, byte[] iv)
        {
            using var aesAlg = Aes.Create();
            aesAlg.Padding = PaddingMode;
            aesAlg.Key = key;
            aesAlg.IV = iv;

            // Create the streams used for decryption.
            using (MemoryStream msDecrypt = new (cipherText.AsArray()))
            {
                using (CryptoStream csDecrypt = new (msDecrypt, aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV), CryptoStreamMode.Read))
                {
                    using (BinaryReader srDecrypt = new (csDecrypt))
                    {
                        return srDecrypt.ReadBytes(cipherText.Length);
                    }
                }
            }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask ProcessMessagesAsync(IoSink<CcProtocBatchJob<chroniton, CcDiscoveryBatch>> batchJob, CcAdjunct @this)
        {
            //IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
            //{
                //var (@this, batchJob) = (ValueTuple< CcAdjunct , IoSink <CcProtocBatchJob<chroniton, CcDiscoveryBatch>>>)state;
                try
                {
                    await @this.ZeroUnBatchAsync(batchJob, static async (batchItem, @this, currentRoute) =>
                    {
                        chroniton zero = default;
                        ByteString payload = null;
                        try
                        {
                            zero = batchItem.Zero;

                            if (zero == null)
                                return;

                            
                            if (zero.Data.Length == 0)
                            {
                                @this._logger.Warn($"Got zero message from {CcDesignation.MakeKey(zero.PublicKey.Memory.AsArray())}");
                                return;
                            }

                            if (!@this.Zeroed() && !currentRoute.Zeroed())
                            {
                                try
                                {
                                    if (zero.Aes > 0)
                                    {
                                        if (!currentRoute.Designation.Primed)
                                            return;

                                        if (currentRoute.Designation.Iv == null)
                                            currentRoute.Designation.SetIv(currentRoute.Designation.PublicKey, @this.Hub.Designation.PublicKey, zero.Aes);

                                        var r = currentRoute.Designation.Round;
                                        do
                                        {
                                            try
                                            {
                                                payload = UnsafeByteOperations.UnsafeWrap(
                                                    AesDecryptFromBytes(zero.Data.Memory,
                                                        currentRoute.Designation.GetRound(zero.Aes),
                                                        currentRoute.Designation.Iv)[..zero.Size]);
                                            }
                                            catch (Exception e) when (!@this.Zeroed())
                                            {
                                                if(@this.Designation.Round > 0)
                                                    @this._logger.Error(e, $"Failed to decrypt aes message!");
                                                @this.Designation.UnPrime();
                                                return;
                                            }
                                            catch when (@this.Zeroed()) { return;}
                                            
                                        } while (r != currentRoute.Designation.Round);

                                        if (payload == null || payload.Length == 0)
                                        {
                                            if(!(@this.Zeroed()))
                                                @this._logger.Fatal($"Unable to decipher message with k = {currentRoute.Designation.GetRound(zero.Aes).HashSig()}[{zero.Aes}], iv = {currentRoute.Designation.Iv?.HashSig()}");
                                            @this.Designation.UnPrime();
                                            return;
                                        }
                                        
                                        zero.Data = payload;
                                    }
                                    else
                                    {
                                        payload = zero.Data;
                                    }

                                    var srcEndPoint = batchItem.EndPoint.GetEndpoint();
                                    //switch ((CcDiscoveries.MessageTypes)MemoryMarshal.Read<int>(packet.Signature.Span))
                                    switch ((CcDiscoveries.MessageTypes)zero.Type)
                                    {
                                        case CcDiscoveries.MessageTypes.Probe:
                                            var probe = CcProbeMessage.Parser.ParseFrom(payload);
                                            if (probe.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                await currentRoute.ProcessAsync(probe, srcEndPoint, zero).FastPath();
                                            break;
                                        case CcDiscoveries.MessageTypes.ProbeResponse:
                                            var probeResponse = CcProbeResponse.Parser.ParseFrom(payload);
                                            if (probeResponse.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                await currentRoute.ProcessAsync(probeResponse, srcEndPoint, zero).FastPath();
                                            break;
                                        case CcDiscoveries.MessageTypes.Scan:
                                            if (!currentRoute.Verified && !@this.CcCollective.ZeroDrone)
                                            {
#if DEBUG
                                                if (@this.IsProxy)
                                                    @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Scan)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}, proxy = {@this.IsProxy}");
#endif
                                                break;
                                            }

                                            var scan = CcScanRequest.Parser.ParseFrom(payload);
                                            if (scan.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                await currentRoute.ProcessAsync(scan, srcEndPoint, zero).FastPath();
                                            break;
                                        case CcDiscoveries.MessageTypes.ScanResponse:
                                            if (!currentRoute.Verified)
                                            {
#if DEBUG
                                                @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.ScanResponse)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                break;
                                            }
                                            var adjunctResponse = CcAdjunctResponse.Parser.ParseFrom(payload);
                                            if (adjunctResponse.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                await currentRoute.ProcessAsync(adjunctResponse, srcEndPoint, zero).FastPath();
                                            break;
                                        case CcDiscoveries.MessageTypes.Fuse:
//                                            if (!currentRoute.Verified)
//                                            {
//#if DEBUG
//                                                @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Fuse)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}; T = {@this.CcCollective.TotalConnections}/{@this.CcCollective.Hub.Neighbors.Count - 1}, in = {@this.CcCollective.IngressCount}, out = {@this.CcCollective.EgressCount}");
//#endif
//                                                if (@this.CcCollective.IngressCount <
//                                                    @this.CcCollective.parm_max_inbound)
//                                                {
//                                                    await @this.Router.ProbeAsync("SYN-TRY", srcEndPoint).FastPath();
//                                                }
//                                                break;
//                                            }
                                            var fuse = CcFuseRequest.Parser.ParseFrom(payload);

                                            if (fuse.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                            {
                                                await currentRoute.ProcessAsync(fuse, srcEndPoint, zero).FastPath();
                                            }
                                            else
                                                @this._logger.Trace($"Slow fuse request[{payload.Memory.PayloadSig()}]({payload.Length}): {fuse.Timestamp.ElapsedUtcMs()} ({DateTimeOffset.FromUnixTimeMilliseconds(fuse.Timestamp)})");

                                            break;
                                        case CcDiscoveries.MessageTypes.FuseResponse:
                                            if (!currentRoute.Verified)
                                            {
#if DEBUG
                                                @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.FuseResponse)}: p = {currentRoute.IsProxy}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                break;
                                            }
                                            var fuseResponse = CcFuseResponse.Parser.ParseFrom(payload);
                                            if (fuseResponse.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                await currentRoute.ProcessAsync(fuseResponse, srcEndPoint, zero).FastPath();
                                            break;
                                        case CcDiscoveries.MessageTypes.Defuse:
                                            if (@this.CcCollective.ZeroDrone || !currentRoute.Verified)
                                            {
#if DEBUG
                                                @this._logger.Warn($"{nameof(CcDiscoveries.MessageTypes.Defuse)}: Unrouted request from {srcEndPoint} ~> {@this.MessageService.IoNetSocket.LocalNodeAddress.IpPort}");
#endif
                                                break;
                                            }

                                            var deFuse = CcDefuseRequest.Parser.ParseFrom(payload);
                                            if (deFuse.Timestamp.ElapsedUtcMs() < @this.parm_max_network_latency_ms * 2)
                                                await currentRoute.ProcessAsync(deFuse, srcEndPoint, zero).FastPath();
                                            break;
                                    }
                                }
                                catch when (@this.Zeroed() || currentRoute.Zeroed()) { }
                                catch (Exception e) when (!@this.Zeroed() && !currentRoute.Zeroed())
                                {
                                    payload ??= ByteString.Empty;
                                    @this._logger?.Error(e, $"{(CcDiscoveries.MessageTypes)zero.Type} [FAILED]: {payload.Memory.PayloadSig()}, l = {payload.Length}, {@this.Key}");
                                }
                            }
                        }
                        catch when (@this.Zeroed()) { }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            payload ??= ByteString.Empty;
                            @this._logger?.Error(e, $"{zero} [FAILED]: l = {payload.Length}, {@this.Key} ({Convert.ToBase64String(payload.Span)})");
                        }
                    }, @this).FastPath();
                }
                finally
                {
                    if (batchJob != null && batchJob.State != IoJobMeta.JobState.Consumed)
                        await batchJob.SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                }
                //}, (@this, batchJob));
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <returns></returns>
        private async ValueTask ProcessDiscoveriesAsync()
        {
            try
            {
                var spinWait = new SpinWait();
                //ensure the channel
                do
                {

                    Interlocked.Exchange(ref _protocolConduit, MessageService.GetConduit<CcProtocBatchJob<chroniton, CcDiscoveryBatch>>(nameof(CcAdjunct)));

                    if (_protocolConduit != null)
                        break;

                    //Get the conduit
                    if(spinWait.Count % 10000 == 0)
                        _logger.Trace($"Waiting for {Description} stream to spin up");
                    
                    spinWait.SpinOnce();
                } while (_protocolConduit == null && !Zeroed());

            
                //fail fast on these
                if(Zeroed() || (_protocolConduit?.Zeroed()??true))
                    return;

                do{
                    //The consumer
                    var width = _protocolConduit.Source.ZeroConcurrencyLevel;
                    for (var i = 0; i < width; i++)
                        await ZeroAsync(static async state =>
                        {
                            var (@this, i) = (ValueTuple<CcAdjunct, int>)state;
                            try
                            {
                                while (!@this.Zeroed())
                                    await @this._protocolConduit.ConsumeAsync(@this.ProcessMessagesAsync, @this).FastPath();
                            }
                            catch when (@this.Zeroed() || @this._protocolConduit?.UpstreamSource == null) { }
                            catch (Exception e) when (!@this.Zeroed() && @this._protocolConduit?.UpstreamSource != null)
                            {
                                @this._logger?.Error(e, $"{@this.Description}");
                            }
                        }, (this,i)).FastPath();
                    
                    //producer;
                    for (var i = 0; i < width ; i++)
                        await ZeroAsync(static async @this =>
                        {
                            try
                            {
                                while (!@this.Zeroed())
                                {
                                    try
                                    {
                                        if (!await @this._protocolConduit.ProduceAsync().FastPath())
                                            await Task.Delay(@this.parm_min_failed_production_time, @this.AsyncTasks.Token);
                                    }
                                    catch when(@this.Zeroed()){}
                                    catch (Exception e) when (!@this.Zeroed())
                                    {
                                        @this._logger.Error(e, $"Production failed for {@this.Description}");
                                        break;
                                    }
                                }
                            }
                            catch when (@this.Zeroed() || @this.Source.Zeroed() || @this._protocolConduit?.UpstreamSource == null) { }
                            catch (Exception e) when (!@this.Zeroed())
                            {
                                @this._logger?.Error(e, $"{@this.Description}");
                            }
                        
                        },this).FastPath();

                    await AsyncTasks.BlockOnNotCanceledAsync().FastPath();
                }
                while (!Zeroed());  
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Debug(e, $"Error processing {Description}");
            }

            if (!Zeroed())
            {
                _logger?.Fatal($"{Description} Stopped prosessing discoveries.");
            }
        }

        /// <summary>
        /// Defuse drop request
        /// </summary>
        /// <param name="request">The request</param>

        /// <param name="src"></param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcDefuseRequest request, IPEndPoint src, chroniton packet)
        {
            if (!Assimilating)
            {
                if(IsProxy)
                    _logger.Error($"{nameof(CcDefuseRequest)}: Ignoring {request.Timestamp.ElapsedMsToSec()}s old/invalid request, {Description}");
                return;
            }
            
            //PAT
            //LastPat = Environment.TickCount;

            if (IsDroneAttached)
            {
                _logger.Trace($"{nameof(CcDefuseRequest)}: Disconnected; {Direction}, {Description}");
                await DetachDroneAsync().FastPath();
            }
            //else
            //{
            //    //Hup
            //    if (Direction == Heading.Undefined && CcCollective.IngressCount < CcCollective.parm_max_inbound)
            //        await ProbeAsync();
            //}
        }

        /// <summary>
        /// Fusing Request message from client
        /// </summary>
        /// <param name="request">The request</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcFuseRequest request, IPEndPoint src, chroniton packet)
        {
            //fail fast
            int sent;
            if (!Assimilating || CcCollective.ZeroDrone)
            {
                //send reject so that the sender's state can be fixed
                var reject = new CcFuseResponse()
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    Accept = false,
                    Src = CcCollective.GossipAddress.IpEndPoint.ToByteString()
                };

                if ((sent = await Router.SendMessageAsync(reject.ToByteArray(), CcDiscoveries.MessageTypes.FuseResponse, src).FastPath()) > 0)
                    _logger.Trace($"<\\- {nameof(CcFuseResponse)} ({sent}): Reply [REJECT] to {src} [OK], {src}, []");
                else
                    _logger.Error($"<\\- {nameof(SendMessageAsync)} ({sent}): Reply [REJECT] to {src} [FAILED], {Description}, {MetaDesc}");

                return;
            }
#if !SLOTS
            Interlocked.Increment(ref _openSlots);
#endif
            //PAT
            //LastPat = Environment.TickCount;

            var fuseResponse = new CcFuseResponse
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Accept = CcCollective.IngressCount < CcCollective.parm_max_inbound && _direction == 0,
                Src = CcCollective.GossipAddress.IpEndPoint.ToByteString()
            };

            if (fuseResponse.Accept)
            {
                var origState = _state.Value;
                var stateIsValid = CompareAndEnterState(AdjunctState.Fusing, AdjunctState.Verified, overrideHung: parm_max_network_latency_ms) == AdjunctState.Verified;

                if (!stateIsValid)
                {
                    //avert deadlock
                    if (origState is >= AdjunctState.Fusing and <= AdjunctState.Connected && CcDesignation.Sha256.ComputeHash(Hub.Designation.PublicKey)[0] < CcDesignation.Sha256.ComputeHash(packet.PublicKey.Memory.AsArray())[0])
                        stateIsValid = true;
                }

                fuseResponse.Accept &= stateIsValid;
                if (!stateIsValid && _state.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                {
                    _logger.Warn($"{Description}: Invalid state ~{_state.Value}, age = {_state.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} -  [RACE OK!]");
                }
            }

            if ((sent= await SendMessageAsync(fuseResponse.ToByteArray(),
                    CcDiscoveries.MessageTypes.FuseResponse).FastPath()) > 0)
            {
                var response = $"{(fuseResponse.Accept ? "accept" : $"reject ({(CcCollective.IngressCount < CcCollective.parm_max_inbound? "available":"full") })")}";

                _logger.Debug(fuseResponse.Accept
                    ? $"# {Description}"
                    : $"<\\- {nameof(CcFuseResponse)} ({sent}): reply = {response}, dest = {Address}, [{Designation}]");

                if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.SendProtoMsg,
                        Msg = new ProtoMsg
                        {
                            CollectiveId = Hub.Router.Designation.IdString(),
                            Id = Designation.IdString(),
                            Type = $"peer response {response}"
                        }
                    });
            }
            else
                _logger.Error($"<\\-  {nameof(CcFuseRequest)}: Send fuse response [FAILED], {Description}, {MetaDesc}");

            if (!CcCollective.ZeroDrone && !fuseResponse.Accept && StochasticRate)
                await SeduceAsync("SYN-FU", IIoSource.Heading.Egress).FastPath();
        }

        /// <summary>
        /// Peer response message from client
        /// </summary>
        /// <param name="response">The request</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcFuseResponse response, IPEndPoint src, chroniton packet)
        {
            var f1 = _fuseRequest.Count;
            var f2 = Router._fuseRequest.Count;
            var key = src.ToString();
            var fuseMessage = await _fuseRequest.ResponseAsync(key, response.ReqHash).FastPath();

            if(!fuseMessage)
                fuseMessage = await Router._fuseRequest.ResponseAsync(key, response.ReqHash).FastPath();

            if (!fuseMessage)
            {
                if (IsProxy)
                {
                    if (response.ReqHash.Length > 0 && _fuseRequest.Count > 0)
                    {
                        _logger.Error($"-/h> {nameof(CcFuseResponse)}({packet.CalculateSize()}, {response.CalculateSize()}, {response.Timestamp.ElapsedUtcMs()}ms) - {packet.Data.Memory.HashSig()}({packet.Data.Length}): No-Hash!!!, r = {response.ReqHash.Memory.HashSig()}({response.ReqHash.Length}), _fuseRequest = {f1}({_fuseRequest.Count}), {f2}({Router._fuseRequest.Count}), l = {MessageService.IoNetSocket.LocalNodeAddress} ~> r = {src}({Address})");
                    }
#if DEBUG
                    _logger.Warn($"-/h> {nameof(CcFuseResponse)}({packet.CalculateSize()}, {response.CalculateSize()}, {response.Timestamp.ElapsedUtcMs()}ms) - {packet.Data.Memory.HashSig()}({packet.Data.Length}): No-Hash!!!, r = {response.ReqHash.Memory.HashSig()}({response.ReqHash.Length}), _fuseRequest = {f1}({_fuseRequest.Count}), {f2}({Router._fuseRequest.Count}), l = {MessageService.IoNetSocket.LocalNodeAddress} ~> r = {src}({Address})");
#endif
                }

                return;
            }

            //PAT
            Interlocked.Decrement(ref _connectionAttempts);

            switch (response.Accept)
            {
                //Race for 
                case true when _state.Value == AdjunctState.Fusing && _direction == 0:
                {
                    Interlocked.Exchange(ref _lastSeduced, Environment.TickCount);
                    Interlocked.Exchange(ref _seduceBurst, 0);

                    if (!await ConnectAsync(response.Src.Memory.AsArray().GetEndpoint()).FastPath())
                    {
                        _logger.Trace($"-/h> {nameof(CcFuseResponse)}: FAILED to connect! Defusing..., {Description}");
                    }

                    break;
                }
                case true:
                case false:
                {
                    AdjunctState oldState;
                    if (_state.Value != AdjunctState.Unverified &&
                        !(_state.Value == AdjunctState.Verified && _state is not { Prev.Value: AdjunctState.Unverified }) &&
                        (oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Fusing)) != AdjunctState.Fusing)
                    {
                        if (oldState != AdjunctState.Connected &&
                            _state.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                        {
                            _logger.Warn($"-/h> {nameof(CcFuseResponse)}(f) - {Description}: Invalid state, {oldState}, age = {_state.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Fusing)}");
                            ResetState(AdjunctState.Verified);
                        }
                    }

                    if (!CcCollective.ZeroDrone)
                        await ScanAsync().FastPath();

                    break;
                }
            }
        }

        public static long ConnectionTime;
        public static long ConnectionCount;

        /// <summary>
        /// Sends a message to the neighbor
        /// </summary>
        /// <param name="data">The message data</param>
        /// <param name="dest">The destination address</param>
        /// <param name="type">The message type</param>
        /// <returns></returns>
        private async ValueTask<int> SendMessageAsync(byte[] data, CcDiscoveries.MessageTypes type = CcDiscoveries.MessageTypes.Undefined, IPEndPoint dest = null)
        {
            try
            {
                if (Zeroed())
                    return 0;
                
                dest ??= Address.IpEndPoint;
                
                if (data == null || data.Length == 0 || Equals(dest, CcCollective.PeerAddress.IpEndPoint))
                    return 0;
                
                chroniton packet = null;
                
                try
                { 
                    packet = _chronitonHeap.Take();

                    if (packet == null)
                        throw new OutOfMemoryException($"{nameof(_chronitonHeap)}: {_chronitonHeap.Description}, {Description}");

                    if (Probed && Designation.Primed)
                    {
                        //init stuff
                        if (Designation.Iv == null)
                            Designation.SetIv(Designation.PublicKey, Hub.Designation.PublicKey, Designation.Round);

                        (packet.Aes, var key) = Designation.GetRound();
                        packet.Data = UnsafeByteOperations.UnsafeWrap(AesEncryptToBytes(data, key, Designation));
                        packet.Size = data.Length;
                    }
                    else
                    {
                        packet.Data = UnsafeByteOperations.UnsafeWrap(data);
                        packet.Aes = 0;
                        packet.Size = data.Length;
                    }
                    
                    packet.Type = (int)type;

                    if (Designation.Primed)
                    {
                        if(packet.Sabot.Length == 0)
                            packet.Sabot = UnsafeByteOperations.UnsafeWrap(Designation.Sabot(packet.Data.Span));
                        else
                            Designation.Sabot(packet.Data.Span, packet.Sabot.Memory.AsArray());
                    }
                    else
                    {
                        //sabot
                        if (packet.Sabot == null || packet.Sabot.Length == 0)
                            packet.Sabot = UnsafeByteOperations.UnsafeWrap(CcDesignation.HashRe(packet.Data.Memory, 0, packet.Data.Length));
                        else
                            CcDesignation.HashRe(packet.Data.Memory, 0, packet.Data.Length, packet.Sabot.Memory.AsArray());
                    }
                    
                    //ed25519
                    if(packet.Signature == null || packet.Signature.Length == 0)
                        packet.Signature = UnsafeByteOperations.UnsafeWrap(CcCollective.CcId.Sign(packet.Data.Memory.AsArray(), 0, packet.Data.Length));
                    else
                        CcCollective.CcId.Sign(packet.Data.Memory.AsArray(), packet.Signature.Memory.AsArray(), 0, packet.Data.Length);

                    Tuple<byte[],byte[]> buf = null;
                    try
                    {
                        buf = _sendBuf.Take();
                        if (buf == null)
                            throw new OutOfMemoryException($"{nameof(_sendBuf)}, {_sendBuf.Description}");

                        var packetLen = packet.CalculateSize();

                        if (packetLen > short.MaxValue)
                            throw new InvalidOperationException($"Max message size supported is {short.MaxValue} bytes, got {packetLen} bytes");

                        packet.WriteTo(buf.Item1.AsSpan(0, packetLen));
                        var length = (ulong)(packetLen + sizeof(ulong));
                        buf.Item1[length] = 0;

                        length = (ulong)LZ4Codec.Encode(buf.Item1,0, packetLen, buf.Item2, sizeof(ulong), buf.Item2.Length - sizeof(ulong));
                        MemoryMarshal.Write(buf.Item2, ref length);
#if TRACE
                        _logger.Trace($"=//> {data.PayloadSig("M")} -> {buf.Item1[..(int)length].PayloadSig()} {type} -> {buf.Item2[..((int)length + sizeof(ulong))].PayloadSig("C")} {type} -> {dest.Url}");
                        //_logger.Trace($"==//> {buf.Item1[..(int)length].Print()}");
                        //_logger.Trace($"==//> {buf.Item2[sizeof(ulong)..(int)length].Print()}");
#endif

                        //Console.WriteLine($"[0] = {buf.Item2[8]}, [1] = {buf.Item2[9]}, [2] = {buf.Item2[10]}, [3] = {buf.Item2[11]}");
                        //Console.WriteLine($"[0] = {buf.Item2[length - 4] + sizeof(ulong)}, [1] = {buf.Item2[length - 3 + sizeof(ulong)]}, [2] = {buf.Item2[length - 2 + sizeof(ulong)]}, [3] = {buf.Item2[length - 1 + sizeof(ulong)]}");
                        //Console.WriteLine($"[0] = {buf.Item1[0]}, [1] = {buf.Item1[1]}, [2] = {buf.Item1[2]}, [3] = {buf.Item1[3]}");
                        //Console.WriteLine($"[0] = {buf.Item1[packetLen - 4]}, [1] = {buf.Item1[packetLen - 3]}, [2] = {buf.Item1[packetLen - 2]}, [3] = {buf.Item1[packetLen - 1]}");
                        //Console.WriteLine($"original len = {packetLen}");
                        //simulate byzantine failure.                
#if LOSS
                        var sent = 0;
                        var loss = 51;
                        if (_random.Next(100) < loss) //drop
                        {
                            sent = msgRaw.Length;
                        } 
                        else if(_random.Next(100) < loss) //duplicate
                        {
                            sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                                .FastPath().ConfigureAwait(ZC);
                            
                            sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                                .FastPath().ConfigureAwait(ZC);
                        }
                        else //nominal
                        {
                            sent = await MessageService.IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length, dest.IpEndPoint)
                                .FastPath().ConfigureAwait(ZC);
                        }

                        return sent;
#else
#if MOCK_ZERO_CONNECTION
                        if (RandomNumberGenerator.GetInt32(0, 100) > 50)
                        {
                            var s = RandomNumberGenerator.GetInt32(20, (int)(length / 2));
                            var s2 = RandomNumberGenerator.GetInt32(s + 1, (int)(length - 20));
                            //Console.WriteLine($"S = {s}/{s2}/{length - sizeof(ulong)}");
                            //Console.WriteLine($"S = {s}/{s2 - s}/{(int)(length - (ulong)s2)}");
                            await MessageService.IoNetSocket.SendAsync(buf.Item2, 0, s, dest.IpEndPoint).FastPath();
                            await MessageService.IoNetSocket.SendAsync(buf.Item2, s, s2 - s, dest.IpEndPoint).FastPath();
                            return await MessageService.IoNetSocket.SendAsync(buf.Item2, s2, (int)(length - (ulong)s2) + sizeof(ulong), dest.IpEndPoint).FastPath();
                        }
                        else
                        {
                            return await MessageService.IoNetSocket.SendAsync(buf.Item2, 0, (int)length + sizeof(ulong), dest.IpEndPoint);
                        }
#else
                        return await MessageService.IoNetSocket.SendAsync(buf.Item2, 0, (int)length + sizeof(long), dest, MemoryMarshal.Read<long>(packet.Sabot.Span)).FastPath();
#endif
                    }
                    finally
                    {   
                        _sendBuf.Return(buf);
                    }
#endif
                }
                catch when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Debug(e, $"{nameof(SendMessageAsync)} Failed to send message {Description}");
                }
                finally
                {
                    if (packet != null)
                    {
                        packet.Data = ByteString.Empty;
                        packet.Signature = ByteString.Empty;
                        _chronitonHeap?.Return(packet);
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Debug(e, $"Failed to send message {Description}");
            }

            return 0;
        }

        /// <summary>
        /// Scan response
        /// </summary>
        /// <param name="response">The response</param>
        /// <param name="src">Endpoint data</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcAdjunctResponse response, IPEndPoint src, chroniton packet)
        {
            try
            {
                var matchRequest = await _scanRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();
                if (!matchRequest)
                    matchRequest = await Router._scanRequest.ResponseAsync(src.ToString(), response.ReqHash).FastPath();

                if (!matchRequest || !Assimilating || response.Contacts.Count > parm_max_swept_drones)
                {
                    if (IsProxy && response.Contacts.Count <= parm_max_swept_drones)
                        _logger.Debug(
                            $"{nameof(CcAdjunctResponse)}{response.ToByteArray().PayloadSig()}: Reject, rq = {response.ReqHash.Memory.HashSig()}, {response.Contacts.Count} > {parm_max_swept_drones}? from {CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString()}, RemoteAddress = {Address}, c = {_scanRequest.Count}, matched[{src}]");
                    return;
                }

                //_logger.Debug($"<\\- {nameof(CcAdjunctResponse)}: Received {response.Contacts.Count} potentials from {Description}");

                Interlocked.Decrement(ref _scanCount);

                //PAT
                //LastPat = Environment.TickCount;

                var processed = 0;
                foreach (var sweptDrone in response.Contacts)
                {
                    //Never add ourselves (by ID)
                    if (sweptDrone.PublicKey.Memory.ArrayEqual(CcCollective.CcId.PublicKey))
                        continue;

                    //Don't add already known neighbors
                    var id = CcDesignation.FromPubKey(sweptDrone.PublicKey.Memory);
                    var idStr = id.ToString();

                    if (Hub.Neighbors.Values.Any(n => ((CcAdjunct)n).Designation.IdString() == idStr && ((CcAdjunct)n).State >= AdjunctState.Verified && ((CcAdjunct)n).SecondsSincePat < CcCollective.parm_mean_pat_delay_s))
                        continue;

                    //ingest the most seductive
                    if (!await Router.ProbeAsync("SYN-DMZ-SCA", sweptDrone.Url.GetEndpoint()).FastPath())
                    {
#if DEBUG
                        _logger.Trace($"-/h> Probing swept {sweptDrone.Url.GetEndpoint()} failed!, {Description}");
#endif
                    }
                    else
                        processed++;
                }

                if (processed > 0)
                {
                    Interlocked.Exchange(ref _scanAge, Environment.TickCount);
                    _logger.Debug($"-/h> {nameof(CcAdjunctResponse)}: {processed}/{response.Contacts.Count} adjuncts; {Description}");
                }
                    
            }
            catch when (Zeroed()){ }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"-/h> {nameof(ProcessAsync)}: [Failed]; {Description}");
            }
        }

        /// <summary>
        /// Collect another adjunct into the collective
        /// </summary>
        /// <param name="newRemoteEp">The location</param>
        /// <param name="designation">The adjunctId</param>
        /// <param name="verified">Whether this designation  has already been verified</param>
        /// <param name="dmz">dmz addr</param>
        /// <param name="session">The session parameter</param>
        /// <returns>A task, true if successful, false otherwise</returns>
        private async ValueTask<bool> CollectAsync(IPEndPoint newRemoteEp, CcDesignation designation, bool verified, IPEndPoint dmz = null, IPEndPoint session = null)
        {
            var success = false;
            try
            {
                var newAdjunct =
                    (CcAdjunct)Hub.MallocNeighbor(Hub, MessageService, (designation, newRemoteEp, verified));
                newAdjunct.Session = session;
                newAdjunct.Dmz = dmz;

                if (Hub.ContainsId(newAdjunct.Designation.IdString()))
                {
                    await newAdjunct.NoRetry().DisposeAsync(this, $"{nameof(CollectAsync)}: fast duplicate").FastPath();
                    return false;
                }

                if (!Zeroed() && await Hub.ZeroAtomicAsync(static async (s, state, ___) =>
                    {
                        var (@this, newAdjunct, verified) = state;

                        if (@this.Hub.ContainsId(newAdjunct.Designation.IdString()))
                        {
                            await newAdjunct.NoRetry().DisposeAsync(@this, $"{nameof(CollectAsync)}: atomic duplicate").FastPath();
                            return false;
                        }

                        try
                        {
                            if (@this.Hub.Neighbors.Count > @this.CcCollective.MaxAdjuncts)
                            {
                                //drop something
                                var bad = @this.Hub.Neighbors.Values.Where(n =>
                                        !((CcAdjunct)n).LivenessTest ||
                                        ((CcAdjunct)n).IsProxy &&
                                        ((CcAdjunct)n).UpTime.ElapsedUtcMs() > @this.parm_min_uptime_ms &&
                                        ((CcAdjunct)n).State < AdjunctState.Verified
                                    )
                                    .OrderByDescending(n => ((CcAdjunct)n).UpTime.ElapsedUtcMs());

                                var good = @this.Hub.Neighbors.Values.Where(n =>
                                            ((CcAdjunct)n).IsProxy &&
                                            ((CcAdjunct)n).UpTime.ElapsedUtcMs() > @this.parm_min_uptime_ms &&
                                            ((CcAdjunct)n).State < AdjunctState.Connected
                                        //&& (@this.CcCollective.ZeroDrone || ((CcAdjunct)n).TotalPats > @this.parm_min_pats_before_shuffle)
                                    )
                                    .OrderBy(n => ((CcAdjunct)n).State < AdjunctState.Verified ? 0 : 1)
                                    .ThenBy(n => ((CcAdjunct)n).Probed ? 1 : 0)
                                    .ThenByDescending(n => ((CcAdjunct)n).ConnectionAttempts)
                                    .ThenBy(n => ((CcAdjunct)n)._openSlots)
                                    .ThenByDescending(n => ((CcAdjunct)n).UpTime.ElapsedUtcMs());

                                var badList = bad.ToList();
                                if (badList.Any())
                                {
                                    var dropped = badList.FirstOrDefault();
                                    if (dropped != default && ((CcAdjunct)dropped).State < AdjunctState.Verified)
                                    {
                                        await ((CcAdjunct)dropped).DisposeAsync(@this,
                                            $"collected - liveness = {((CcAdjunct)dropped).LivenessTest}").FastPath();
                                        @this._logger.Trace(
                                            $"@ liveness = {((CcAdjunct)dropped).LivenessTest}; {dropped.Description}");
                                    }
                                }
                                else //try harder
                                {
                                    var c = 2;
                                    foreach (var ioNeighbor in good)
                                    {
                                        if (((CcAdjunct)ioNeighbor).State < AdjunctState.Connecting)
                                        {
                                            @this._logger.Trace($"@ {ioNeighbor.Description}");
                                            await ioNeighbor.DisposeAsync(@this, "collected").FastPath();
                                            if (c-- == 0)
                                                break;
                                        }
                                    }
                                }
                            }

                            if (@this.Hub.Neighbors.Count > @this.CcCollective.MaxAdjuncts)
                                return false;

                            //Transfer?
                            retry:
                            if (@this.Hub.ContainsId(newAdjunct.Designation.IdString()))
                            {
                                await newAdjunct.NoRetry().DisposeAsync(@this, $"{nameof(CollectAsync)}: slow duplicate").FastPath();
                                return false;
                            }

                            if (!@this.Hub.Neighbors.TryAdd(newAdjunct.Key, newAdjunct))
                            {
                                if (@this.Hub.Neighbors.TryGetValue(newAdjunct.Key, out var dup))
                                {
                                    await newAdjunct.DisposeAsync(@this,
                                            $"was {(verified ? "egress" : "ingress")}, serial = {newAdjunct.Serial}; LOST RACE TO = {dup.Description}; c = {@this.Hub.Neighbors.Count}/{@this.CcCollective.MaxAdjuncts}, state = {newAdjunct.State}, v = {verified}")
                                        .FastPath();
                                    return false;
                                }

                                goto retry;
                            }

                            @this._logger.Debug($"* {nameof(CollectAsync)}: {newAdjunct.Description}");
                            if (verified)
                                await @this.RouteAsync(newAdjunct.Address.IpEndPoint,
                                    ByteString.CopyFrom(newAdjunct.Designation.PublicKey)).FastPath();

                            newAdjunct.LastPat = Environment.TickCount;
                            return true;
                        }
                        catch when (@this.Zeroed())
                        {
                        }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger.Error(e, $"{@this.Description ?? "N/A"}");
                        }

                        return false;
                    }, (this, newAdjunct, verified)).FastPath())
                {
                    await newAdjunct.SeduceAsync("SYN-ACK", verified ? IIoSource.Heading.Both : IIoSource.Heading.Ingress).FastPath();

                    await newAdjunct.BlockOnReplicateAsync().FastPath();

                    return true;
                }
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(CollectAsync)}: [FAILED]; {Description}");
            }
            finally
            {
                if (!success && RetryOnDc)
                {
                    await BackOffAsync;
                    if(!verified)
                        success = await Router.ProbeAsync("SYN-RE", newRemoteEp, designation.IdString()).FastPath();
                }
            }
            return success;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="src"></param>
        /// <param name="packet"></param>
        /// <returns></returns>
        private async ValueTask ProcessAsync(CcScanRequest request, IPEndPoint src, chroniton packet)
        {
            var sweptResponse = new CcAdjunctResponse
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                Contacts = {}
            };

            var count = 0;
            var certified = Hub.Neighbors.Values.Where(
                    n =>
                        n != this &&
                        //((CcAdjunct)n).LivenessTest &&
                        ((CcAdjunct)n).Assimilating)
                .OrderByDescending(n => ((CcAdjunct)n)._probed)
                .ThenByDescending(n => ((CcAdjunct)n).IsDroneConnected ? 0 : 1)
                .ThenByDescending(n => ((CcAdjunct)n)._openSlots).ToList();

            if (!certified.Any())
            {
                certified = Hub.Neighbors.Values.Where(
                        n => n != this && ((CcAdjunct)n).IsProxy && ((CcAdjunct)n).MessageService?.IoNetSocket?.RemoteNodeAddress?.IpEndPoint != null)
                    .OrderByDescending(n => ((CcAdjunct)n).UpTime.ElapsedUtcMs()).ToList();
            }

            foreach (var ioNeighbor in certified.Take((int)parm_max_swept_drones))
            {
                try
                {
                    sweptResponse.Contacts.Add(new CcAdjunctModel
                    {
                        PublicKey = UnsafeByteOperations.UnsafeWrap(((CcAdjunct) ioNeighbor).Designation.PublicKey),
                        Url = ((CcAdjunct)ioNeighbor).Dmz.ToByteString()
                    });
                }
                catch when (ioNeighbor.Zeroed()){}
                catch (Exception e) when (!ioNeighbor.Zeroed())
                {
                    _logger.Error(e, $"{nameof(CcAdjunctResponse)}: Failed to add adjunct, contacts = {sweptResponse.Contacts}, pk = {((CcAdjunct)ioNeighbor)?.Designation?.PublicKey}, r ={Address} ");
                    continue;
                }

                if (CcCollective.ZeroDrone)
                {
                    Interlocked.Exchange(ref ((CcAdjunct)ioNeighbor)._openSlots,((CcAdjunct)ioNeighbor)._openSlots / 2);
                }
                count++;
            }

            int sent;
            var dst = IsProxy ? Address.IpEndPoint : src;
            if ((sent = await SendMessageAsync(sweptResponse.ToByteArray(), CcDiscoveries.MessageTypes.ScanResponse, dst).FastPath()) > 0)
            {
                if (count > 0)
                {
                    _logger.Trace($"<\\- Broadcast({sent}): {count} adjuncts; {(IsProxy ? Address.Key : src)}");

                    //Emit message event
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "discovery response"
                            }
                        });
                }
            }
            else
            {
                _logger.Debug($"<\\- {nameof(CcScanRequest)}({sent}): Send [FAILED], {Description}, {MetaDesc}");
            }
        }

        [Flags]
        enum DroneStatus
        {
            Undefined = 0,
            Drone = 1 << 0,
        }

        private volatile bool _eventStreamAdded;
        //private byte[] _iv;
        //private static ICryptoTransform _encrypt;
        //private static ICryptoTransform _decrypt;

        /// <summary>
        /// Probe message
        /// </summary>
        /// <param name="probeMessage">The probe packet</param>
        /// <param name="remoteEp">The originating src</param>
        /// <param name="packet">The original packet</param>
        private async ValueTask ProcessAsync(CcProbeMessage probeMessage, IPEndPoint remoteEp, chroniton packet)
        {
            //Drop old API calls
            if(probeMessage.Protocol != parm_protocol_version)
                return;

            //Drop if we are saturated
            if (!IsProxy && !CcCollective.ZeroDrone && CcCollective.IngressCount >= CcCollective.parm_max_inbound && CcCollective.EgressCount >= CcCollective.parm_max_outbound && CcCollective.Hub.Neighbors.Count > CcCollective.parm_max_adjunct && _random.Next(0, 9) < 6)
            {
                _logger.Trace($"{nameof(CcProbeMessage)}: dropped probe from {remoteEp}; [FULL]");
                return;
            }

            //liveness boost
            if(probeMessage.Lamport > 0 && probeMessage.Lamport <= CcCollective.Lamport * 2)
                Interlocked.Exchange(ref _lamport, probeMessage.Lamport);

            CcProbeResponse probeResponse;
            try
            {
                probeResponse = new CcProbeResponse
                {
                    Protocol  = parm_protocol_version,
                    ReqHash   = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),
                    Session   = remoteEp.ToByteString(),
                    OrigDst   = probeMessage.Dst,
                    Src       = CcCollective.PeerAddress.IpEndPoint.ToByteString(),
                    Status    = (long)(IsDroneAttached? DroneStatus.Drone : DroneStatus.Undefined),
#if DEBUG
                    DbgPtr    = GCHandle.ToIntPtr(GCHandle.Alloc(this, GCHandleType.WeakTrackResurrection)).ToInt64() 
#endif
                };
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            //PROCESS DMZ-SYN
            var sent = 0;
            if (!IsProxy)
            { 
                //TODO: sec; remove port from the filter
                var vKey = remoteEp.ToString();
                long spam;
                
                if (_v.TryAdd(vKey, spam = Environment.TickCount) || _v.TryGetValue(vKey, out spam) && (spam & 0xffff).ElapsedMs() > parm_max_v_heuristic_ms || (spam & 0xffff0000) >> (sizeof(int) << 3) < parm_max_v_rate )
                {
                    _v[vKey] = _v[vKey] & 0xffff0000 | (uint)Environment.TickCount;
                    _v[vKey] |= ((spam & 0xffff0000) >> (sizeof(int) << 3) + 1)<< (sizeof(int) << 3);

                    //SEND SYN-ACK
                    byte[] payload;
                    probeResponse.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if ((sent = await SendMessageAsync(payload = probeResponse.ToByteArray(), CcDiscoveries.MessageTypes.ProbeResponse, remoteEp).FastPath()) > 0)
                    {
                        _v.TryUpdate(vKey, Environment.TickCount & (++spam << (sizeof(int) * 8)), spam);

                        var ccId = CcDesignation.FromPubKey(packet.PublicKey.Memory);
#if DEBUG
                        _logger.Trace($"<\\- {nameof(CcProbeResponse)} ({sent}) [{payload.PayloadSig()} ~ {probeResponse.ReqHash.Memory.HashSig()}]: [[SYN-ACK]], dest = {remoteEp}, [{ccId.IdString()}]");
#endif

                        //if (!Equals(remoteEp, probeMessage.Src.GetEndpoint()))
                        //    Console.WriteLine($"ROUTER({CcCollective.PeerAddress}): {remoteEp} <-> {probeMessage.Src.GetEndpoint()}: dst = {probeMessage.Dst.GetEndpoint()}");

                        if (!Hub.ContainsId(ccId.IdString()))
                        {
                            if (!await CollectAsync(remoteEp, ccId, false, probeMessage.Src.GetEndpoint()).FastPath())
                            {
#if DEBUG
                                _logger.Trace($"{nameof(CcProbeResponse)}: Collecting Ingress {remoteEp} failed!, {Description}");
#endif
                                if (RetryOnDc)
                                {
                                    await BackOffAsync;
                                    await Router.ProbeAsync("SYN-REE", probeMessage.Src.GetEndpoint());
                                }
                            }
                            else if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                                {
                                    EventType = AutoPeerEventType.SendProtoMsg,
                                    Msg = new ProtoMsg
                                    {
                                        CollectiveId = Hub.Router.Designation.IdString(),
                                        Id = CcDesignation.MakeKey(packet.PublicKey),
                                        Type = "pong"
                                    }
                                });
                        }

                        //flush v tables
                        if (_v.Count > CcCollective.parm_max_adjunct * 2)
                        {
                            Interlocked.Exchange(ref _vFlush, Environment.TickCount);
                            foreach (var i in _v)
                            {
                                if ((i.Value & 0xffff).ElapsedMs() > parm_max_v_heuristic_ms)
                                    _v.TryRemove(i.Key, out _);
                            }
                        }
                    }
                }
                else if(_v.Count > CcCollective.parm_max_adjunct * 2 || _vFlush.ElapsedMs() > parm_max_v_heuristic_ms)
                {
                    //flush v tables
                    Interlocked.Exchange(ref _vFlush, Environment.TickCount);
                    foreach (var i in _v)
                    {
                        if ((i.Value & 0xffff).ElapsedMs() > parm_max_v_heuristic_ms)
                            _v.TryRemove(i.Key, out _);
                    }
                }
                else if(CcCollective.Neighbors.Count <= CcCollective.EgressCount)
                {
                    IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                    {
                        var (@this, dest) = (ValueTuple<CcAdjunct, IPEndPoint>)state;
                        try
                        {
                            await @this.BackOffAsync;

                            if (@this.CcCollective.TotalConnections >= @this.CcCollective.parm_max_outbound)
                                return;

                            if (!await @this.Router.ProbeAsync("SYN-DC", dest))
                                @this._logger.Trace($"-/> {nameof(CcProbeMessage)} SYN-DC failed; {dest}");
                        }
                        catch (TaskCanceledException){}
                        catch when(@this.Zeroed()){}
                        catch (Exception e) when (!@this.Zeroed()) 
                        {
                            @this._logger.Error(e,$"{nameof(@this.Router.ProbeAsync)}:");
                        }
                    }, (this, remoteEp));
                }
                return;
            }
            
            //PROCESS SYN
            //LastPat = Environment.TickCount;
#if SLOTS
                Interlocked.Exchange(ref _openSlots, probeMessage.Slots);
#endif

            probeResponse.Nsec = UnsafeByteOperations.UnsafeWrap(Designation.PrimedSabot);
            probeResponse.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            if ((sent = await SendMessageAsync(probeResponse.ToByteArray(), type: CcDiscoveries.MessageTypes.ProbeResponse).FastPath()) > 0)
            {
                try
                {
#if DEBUG
                    var data = probeResponse.ToByteArray();
                    _logger.Trace($"<\\- {nameof(CcProbeResponse)} ({sent}) [{data[..data.Length].PayloadSig()} ~ {probeResponse.ReqHash.Memory.HashSig()}]: [[SYN-ACK]], dest = {MessageService.IoNetSocket.RemoteNodeAddress}, [{Designation.IdString()}]");
#endif

                    Interlocked.Increment(ref _probed);

                    if (!CcCollective.ZeroDrone)
                        await SeduceAsync("SYN-DELTA", IIoSource.Heading.Egress).FastPath();

                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "pong"
                            }
                        });
                }
                catch when(Zeroed()){}
                catch (Exception e)when (!Zeroed())
                {
                    _logger?.Error(e,$"<\\- {nameof(SeduceAsync)}: sent = {sent}");
                }
            }
            else if(!Zeroed())
            {
                _logger.Trace($"<\\- {nameof(CcProbeMessage)}: Send({sent}) [FAILED] - [[SYN ACK/KEEP-ALIVE]], to = {Description}");
            }
        }

        /// <summary>
        /// Prepares the services for this adjunct
        /// </summary>
        /// <param name="packet"></param>
        /// <param name="localEp"></param>
        /// <param name="remoteEp"></param>
        /// <returns></returns>
        //private CcProbeResponse PrepareServices(chroniton packet, IPEndPoint localEp, IPEndPoint remoteEp)
        //{
        //    //TODO optimize
        //    var gossipAddress =
        //        Hub.Services.CcRecord.Endpoints[CcService.Keys.gossip];
        //    var peeringAddress =
        //        Hub.Services.CcRecord.Endpoints[CcService.Keys.peering];
        //    var fpcAddress =
        //        Hub.Services.CcRecord.Endpoints[CcService.Keys.fpc];

        //    var probedMessage = new CcProbeResponse
        //    {
        //        Protocol = parm_protocol_version,
        //        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        //        ReqHash = UnsafeByteOperations.UnsafeWrap(CcDesignation.Sha256.ComputeHash(packet.Data.Memory.AsArray())),

        //        //DstAddr = $"{remoteEp.Address}", //TODO, add port somehow
        //        //Services = _serviceMapLocal = new CcServiceMapModel
        //        //{
        //        //    Map =
        //        //    {
        //        //        {
        //        //            CcService.Keys.peering.ToString(),
        //        //            new NetworkAddress { Network = "udp", Port = (uint)peeringAddress.Port }
        //        //        },
        //        //        {
        //        //            CcService.Keys.gossip.ToString(),
        //        //            new NetworkAddress { Network = "tcp", Port = (uint)gossipAddress.Port }
        //        //        },
        //        //        {
        //        //            CcService.Keys.fpc.ToString(),
        //        //            new NetworkAddress { Network = "tcp", Port = (uint)fpcAddress.Port }
        //        //        }
        //        //    }
        //        //}
        //    };
        //    return probedMessage;
        //}

        /// <summary>
        /// Probed message
        /// </summary>
        /// <param name="response">The Probed packet</param>
        /// <param name="remoteEp">Sender</param>
        /// <param name="packet">The original packet</param>
        /// <param name="matched">Whether this response has been matched</param>
        private async ValueTask ProcessAsync(CcProbeResponse response, IPEndPoint remoteEp, chroniton packet, bool matched = false)
        {
            try
            {
                
                if (!matched)
                {
                    var matchRequest = await _probeRequest.ResponseAsync(remoteEp.ToString(), response.ReqHash).FastPath();

                    //Try the router
                    if (!matchRequest && IsProxy && Hub != null)
                        matchRequest = await Hub.Router._probeRequest.ResponseAsync(remoteEp.ToString(), response.ReqHash).FastPath();

                    if (!matchRequest)
                    {
#if DEBUG
                        if (IsProxy && !Zeroed() && _probeRequest.Count > 0)
                        {
                            _logger.Error(
                                $"-/> {nameof(CcProbeResponse)} {packet.Data.Memory.PayloadSig()},{response.ReqHash.Memory.HashSig()}: SEC! age = {response.Timestamp.ElapsedUtcMs()}ms, matcher = ({_probeRequest.Count}, {Router._probeRequest.Count}), d = {_probeRequest.Count}, pats = {TotalPats},  " +
                                $"PK={Designation.IdString()} != {CcDesignation.MakeKey(packet.PublicKey)} (proxy = {IsProxy}),  ssp = {SecondsSincePat}, d = {(AttachTimestamp > 0 ? (AttachTimestamp - LastPat).ToString() : "N/A")}, v = {Verified}, s = {remoteEp}");
                            _probeRequest.DumpToLog();
                        }
#endif
                        return;
                    }

                    Interlocked.Exchange(ref _zeroProbes, 0);
                }

                //Process SYN-ACK
                if (!IsProxy)
                {
                    if (Hub.Neighbors.Count <= CcCollective.MaxAdjuncts)
                    {
                        var ccId = CcDesignation.FromPubKey(packet.PublicKey.Memory);

                        if (!Hub.ContainsId(ccId.IdString()))
                        {
                            //if (!Equals(remoteEp, response.Src.GetEndpoint()))
                            //    Console.WriteLine($"PROXY({CcCollective.PeerAddress}): {remoteEp} <-> {response.Src.GetEndpoint()} == {response.OrigDst.GetEndpoint()}");

                            if (!await CollectAsync(remoteEp, ccId, true, response.Src.GetEndpoint(),
                                    response.Session.GetEndpoint()).FastPath())
                            {
#if DEBUG
                                _logger.Trace(
                                    $"{nameof(CcProbeResponse)}: Collecting Egress {response.Src.GetEndpoint()} failed!, src = {remoteEp}, id = {CcDesignation.FromPubKey(packet.PublicKey.Memory).IdString()}");
#endif
                            }
                        }
                    }
                    
                    return;
                }

                LastPat = Environment.TickCount;

                if (!Verified) //Process ACK
                {
                    AdjunctState oldState;
                    if ((oldState = CompareAndEnterState(AdjunctState.Verified, AdjunctState.Unverified)) != AdjunctState.Unverified)
                    {
                        _logger.Warn($"{nameof(CcProbeResponse)} - {Description}: Invalid state, {oldState}. Wanted {nameof(AdjunctState.Unverified)}");
                        return;
                    }

                    if(CcCollective.ZeroDrone)
                        _logger.Warn($"Verified with queen `{remoteEp}'");
                }

                if(response.Nsec.Length > 0)
                    Designation.EnsureSabot(response.Nsec.Memory.AsArray());

                Session ??= response.Session.GetEndpoint();
                Dmz ??= response.Src.GetEndpoint();

                if (Interlocked.Increment(ref _seduceBurst) >= parm_max_seduce_burst)
                {
                    Interlocked.Exchange(ref _lastSeduced, Environment.TickCount);
                    Interlocked.Exchange(ref _seduceBurst, 0);
                }
                Interlocked.Decrement(ref _zeroProbes);

#if TRACE
                _logger.Trace($"|\\- {nameof(CcProbeResponse)} [{response.ToByteArray().PayloadSig()} ~ {response.ReqHash.Memory.HashSig()}]: Processed <<ACK>>; stealth = ({_lastDeltaSent.ElapsedMs()}/{parm_max_network_latency_ms}) ms {Description}");
#endif
                if (!CcCollective.ZeroDrone)
                    await SeduceAsync("SYN-POK", CcCollective.EgressCount >= CcCollective.parm_max_outbound? IIoSource.Heading.Ingress: IIoSource.Heading.Egress).FastPath();

                //eradicate zombie drones
                if (((DroneStatus)response.Status).HasFlag(DroneStatus.Drone) && !IsDroneAttached && UpTime.ElapsedUtcMs() > parm_min_uptime_ms)
                    await DetachDroneAsync().FastPath();
            }
            catch when(Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(ProcessAsync)}: FAILED! - {Description}");
            }
        }

        /// <summary>
        /// Seduces another adjunct
        /// </summary>
        /// <returns>A valuable task</returns>s
        public async ValueTask<bool> SeduceAsync(string desc, IIoSource.Heading heading, IoNodeAddress dmzEndpoint = null)
        {
            var success = false;
            if (Zeroed() || IsDroneAttached || _state.Value > AdjunctState.Connecting || _lastSeduced.ElapsedMs() < parm_max_network_latency_ms>>1)
                return false;

            try
            {
                if (!CcCollective.ZeroDrone &&
                    heading.HasFlag(IIoSource.Heading.Ingress) &&
                    CcCollective.IngressCount < CcCollective.parm_max_inbound)
                {
                    var proxy = dmzEndpoint == null ? this : Router;
                    //delta trigger

                    if (!await proxy.ProbeAsync(desc, dmzEndpoint?.IpEndPoint).FastPath())
                        _logger.Trace($"<\\- {nameof(ProbeAsync)}({desc}): [FAILED] to seduce {Description}");

                    success = true;
                }

                if (!CcCollective.ZeroDrone &&
                    heading > IIoSource.Heading.Ingress &&
                    Direction == IIoSource.Heading.Undefined &&
                    CcCollective.EgressCount < CcCollective.parm_max_outbound)
                {
                    return success |= Fuse();
                }
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e)when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(SeduceAsync)}: ");
            }

            return success;
        }

        /// <summary>
        /// Sends a probe to scout ahead for adjuncts
        /// </summary>
        /// <param name="desc">Probe description</param>
        /// <param name="dest">The destination address</param>
        /// <param name="id">Optional Id of the node pinged</param>
        /// <returns>Task</returns>
        public async ValueTask<bool> ProbeAsync(string desc, IPEndPoint dest = null, string id = null)
        {
            try
            {
                if (Zeroed())
                    return false;

                dest ??= Address.IpEndPoint;
                
                //Create the ping request
                var probeMessage = new CcProbeMessage
                {
                    Protocol = parm_protocol_version,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Slots = CcCollective.MaxDrones - CcCollective.TotalConnections,
                    Lamport = CcCollective.MaxReq,
                    Src =  CcCollective.PeerAddress.IpEndPoint.ToByteString(),
                    Dst = dest.ToByteString()
                };

                var probeMsgBuf = probeMessage.ToByteArray();

                IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                if ((challenge = await _probeRequest.ChallengeAsync(dest.ToString(), probeMsgBuf).FastPath()) == null)
                {
                    if(!Zeroed())
                        _logger.Fatal($"{nameof(ProbeAsync)} No challange, {_probeRequest.Count}/{_probeRequest.Capacity}");
                    return false;
                }

                // Is this a routed request?
                if (IsProxy)
                {
                    if (ZeroProbes > parm_zombie_max_connection_attempts)
                    {
                        await DisposeAsync(this, $"wd: s = {SecondsSincePat}, probed = {ZeroProbes} << {parm_zombie_max_connection_attempts}, T = {TimeSpan.FromMilliseconds(UpTime.ElapsedUtcMs()).TotalMinutes:0.0}min");
                        return false;
                    }

                    //send
                    var sent = await SendMessageAsync(data: probeMsgBuf, type: CcDiscoveries.MessageTypes.Probe).FastPath();
                    if (sent > 0)
                    {
                        Interlocked.Increment(ref _zeroProbes);
#if DEBUG
                        try
                        {
                            _logger.Trace($"+/> {nameof(ProbeAsync)} ({sent}) [{probeMsgBuf.PayloadSig()}]: [[{desc}]] {MessageService.IoNetSocket.RemoteNodeAddress} - [{Designation.IdString()}]");
                        }
                        catch when (Zeroed()){}
                        catch (Exception e)when(!Zeroed())
                        {
                            _logger.Trace(e);
                        }
#endif
                        if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = Designation.IdString(),
                                    Type = "ping"
                                }
                            });

                        return true;
                    }
                    else
                    {
#if DEBUG
                        _logger.Trace($"+/> {nameof(ProbeAsync)} ({sent}) [{probeMsgBuf.PayloadSig()} ~ {challenge.Value.Hash.HashSig()}]: Send [FAILED] [[{desc}]], [{Description}]");
#endif
                    }

                    await _probeRequest.RemoveAsync(challenge).FastPath();

                    if(!Zeroed() && sent != -1)
                        _logger.Error($"+/> {nameof(ProbeAsync)}: [FAILED], {Description}");

                    return false;
                }

                if(!Zeroed()) //The destination state was undefined, this is local
                {
                    var sent = await SendMessageAsync(probeMsgBuf, CcDiscoveries.MessageTypes.Probe, dest).FastPath();

                    if (sent > 0)
                    {
#if DEBUG
                        _logger.Trace($"+/> {nameof(ProbeAsync)} ({sent}) [{probeMsgBuf.PayloadSig()} {challenge.Value.Hash.HashSig()}]: [[{desc}]], dest = {dest}, [{id}]");
#endif
                        if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = Hub.Router.Designation.IdString(),
                                    Id = id??Designation.IdString(),
                                    Type = "ping"
                                }
                            });

                        return true;
                    }

                    await _probeRequest.RemoveAsync(challenge).FastPath();

                    if(!Zeroed() && sent != -1)
                        _logger.Error($"+/> {nameof(ProbeAsync)}:({desc}) [FAILED], {Description}");

                    return false;
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Debug(e,$"ERROR z = {Zeroed()}, dest = {dest}, source = {MessageService}, _scanRequest = {_scanRequest.Count}");
            }

            return false;
        }

        /// <summary>
        /// Scans adjuncts for more
        /// </summary>
        /// <returns>Task</returns>
        public async ValueTask<bool> ScanAsync()
        {
            try
            {
                //insane checks for proxy scanning only
                if (IsProxy)
                {
                    if (!Assimilating)
                    {
                        _logger.Trace($"{nameof(ScanAsync)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}");
                        return false;
                    }

                    if (_scanCount > parm_zombie_max_connection_attempts)
                    {
                        _logger.Trace($"{nameof(ScanAsync)}: [skipped], no replies {Description}, s = {State}, a = {Assimilating}");

                        if (!IsDroneAttached)
                        {
                            await DisposeAsync(this, $"{nameof(ScanAsync)}: Unable to scan adjunct, count = {_scanCount} << {parm_zombie_max_connection_attempts}").FastPath();
                            return false;
                        }

                        return false;
                    }

                    //if (cooldown == -1)
                    //    cooldown = CcCollective.parm_mean_pat_delay_s * 1000 / 5;

                    ////rate limit
                    //if (cooldown != 0 && ScanAge.ElapsedMs() < cooldown)
                    //    return false;
                }

                var sweepMessage = new CcScanRequest
                {
                     Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var sweepMsgBuf = sweepMessage.ToByteArray();

                IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                if ((challenge = await _scanRequest.ChallengeAsync(Address.IpPort, sweepMsgBuf).FastPath()) == null)
                {
                    _logger.Fatal($"{ScanAsync()}: No Challenge, {_scanRequest.Description}");
                    return false;
                }

                var sent = await SendMessageAsync(sweepMsgBuf,CcDiscoveries.MessageTypes.Scan, Address.IpEndPoint).FastPath();
                if (sent > 0)
                {
                    Interlocked.Increment(ref _scanCount);

#if TRACE
                    _logger.Trace($"+/> {nameof(CcScanRequest)} ({sent}) {sweepMsgBuf.PayloadSig()}: {RemoteAddress}, [{Designation.IdString()}]");
#endif

                    //Emit message event
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = "scan request"
                            }
                        });

                    return true;
                }

                await _scanRequest.RemoveAsync(challenge).FastPath();
                if(!Zeroed() && sent != -1)
                    _logger.Error($"+/> {nameof(CcScanRequest)}: [FAILED], {Description} ");
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e,$"{nameof(CcScanRequest)}: [ERROR] z = {Zeroed()}, state = {State}, dest = {Address}, source = {MessageService}, _scanRequest = {_scanRequest.Count}");
            }

            return false;
        }

        /// <summary>
        /// Causes adjunct to fuse with another adjunct forming a drone. Drones are useful. 
        /// </summary>
        /// <returns>Task</returns>
        public bool Fuse()
        {
            if (IsDroneAttached || CcCollective.ZeroDrone || _state.Value == AdjunctState.Connected || !Probed || CcCollective.EgressCount >= CcCollective.parm_max_outbound)
            {
                if (!CcCollective.ZeroDrone && Probed)
                    _logger.Warn($"{nameof(Fuse)}: [ABORTED], {Description}, s = {State}, a = {Assimilating}, D = {IsDroneConnected}, d = {IsDroneAttached}");
                return false;
            }

            if (_state.Value <= AdjunctState.Unverified) return true;

            AdjunctState oldState;

            var stateIsValid = (oldState = CompareAndEnterState(AdjunctState.Fusing, AdjunctState.Verified, overrideHung: parm_max_network_latency_ms)) == AdjunctState.Verified;
            if (!stateIsValid)
            {
                if (_state.Value is >= AdjunctState.Fusing and <= AdjunctState.Connected && _state.EnterTime.ElapsedMs() > parm_max_network_latency_ms)
                    _logger.Warn($"{nameof(Fuse)} - {Description}: Invalid state, {oldState}, age = {_state.EnterTime.ElapsedMs()}ms. Wanted {nameof(AdjunctState.Verified)} - [RACE OK!] ");
                return false;
            }

            IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
            {
                var @this = (CcAdjunct)state;
                try
                {
                    if(@this.Zeroed())
                        return;

                    await @this.BackOffAsyncSmall;

                    var fuseRequest = new CcFuseRequest
                    {
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Src = @this.CcCollective.GossipAddress.IpEndPoint.ToByteString()
                    };
                    var fuseRequestBuf = fuseRequest.ToByteArray();

                    IoQueue<IoZeroMatcher.IoChallenge>.IoZNode challenge;
                    if ((challenge = await @this._fuseRequest.ChallengeAsync(@this.Address.IpPort, fuseRequestBuf).FastPath()) == null)
                    {
                        @this._logger.Error($"{nameof(_fuseRequest.ChallengeAsync)}: [FAILED]; {@this._fuseRequest.Description}");
                        return;
                    }

                    var sent = await @this.SendMessageAsync(fuseRequestBuf, CcDiscoveries.MessageTypes.Fuse).FastPath();
                    if (sent > 0)
                    {
                        Interlocked.Increment(ref @this._connectionAttempts);
                        @this._logger.Debug($"+/> {nameof(CcFuseRequest)} ({sent}) [{fuseRequestBuf.PayloadSig()}]: {@this.Address}, [{@this.Designation.IdString()}]");
                        
                        if (!@this.CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                            AutoPeeringEventService.AddEvent(new AutoPeerEvent
                            {
                                EventType = AutoPeerEventType.SendProtoMsg,
                                Msg = new ProtoMsg
                                {
                                    CollectiveId = @this.Hub.Router.Designation.IdString(),
                                    Id = @this.Designation.IdString(),
                                    Type = "peer request"
                                }
                            });

                        return;
                    }

                    await @this._fuseRequest.RemoveAsync(challenge).FastPath();

                    if (!@this.Zeroed() && sent != -1 )
                        @this._logger.Error($"+/> {nameof(CcFuseRequest)}: [FAILED], {@this.Description}, {@this.MetaDesc}");
                }
                catch when (@this.Zeroed()) { }
                catch (Exception e) when (!@this.Zeroed())
                {
                    @this._logger.Error(e, $"{nameof(CcFuseRequest)}: [FAILED], {@this.Description}, {@this.MetaDesc}");
                }
            }, this);

            return true;
        }

        /// <summary>
        /// Causes adjuncts to de fuse
        /// </summary>
        /// <returns></returns>
        internal async ValueTask<bool> DeFuseAsync(IoNodeAddress dest = null)
        {
            try
            {
                dest ??= Address;

                var dropRequest = new CcDefuseRequest
                {
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                };

                var sent = await SendMessageAsync(dropRequest.ToByteArray(), CcDiscoveries.MessageTypes.Defuse, dest.IpEndPoint).FastPath();

#if DEBUG
                if (!Zeroed())
                {
                    _logger.Trace(
                        (sent) > 0
                            ? $"-/> {nameof(CcDefuseRequest)} ({sent}): {Address}, [{Designation.IdString()}]"
                            : $"-/> {nameof(CcDefuseRequest)}: [FAILED], {Description}, {MetaDesc}");
                }
#endif

                if (sent > 0)
                {
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.SendProtoMsg,
                            Msg = new ProtoMsg
                            {
                                CollectiveId = Hub.Router.Designation.IdString(),
                                Id = Designation.IdString(),
                                Type = $"peer disconnect"
                            }
                        });

                    return true;
                }
            }
            catch (Exception) when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Debug(e, $"{nameof(DeFuseAsync)}: [ERROR], {Description}, s = {State}, a = {Assimilating}, p = {IsDroneConnected}, d = {dest}, s = {MessageService}");
            }

            return false;
        }

        //TODO complexity
        /// <summary>
        /// Attaches a gossip drone to this neighbor
        /// </summary>
        /// <param name="ccDrone">The drone</param>
        /// <param name="direction"></param>
        public bool AttachDrone(CcDrone ccDrone, IIoSource.Heading direction)
        {
            var success = false;
            try
            {
                //raced?
                if (IsDroneAttached || Zeroed() || CcCollective.TotalConnections > CcCollective.MaxDrones)
                    return false;

                //Race for direction
                if (Interlocked.CompareExchange(ref _direction, (int)direction, (int)IIoSource.Heading.Undefined) !=
                    (int)IIoSource.Heading.Undefined)
                {
                    _logger.Warn($"oz: race for {direction} lost {ccDrone.Description}, current = {Direction}, {_drone?.Description}");
                    return false;
                }

                if (CompareAndEnterState(AdjunctState.Connected, AdjunctState.Connecting) != AdjunctState.Connecting)
                {
                    _logger.Trace($"{nameof(AttachDrone)}: [LOST] {ccDrone?.Description}");
                    return false;
                }

                if (Direction == IIoSource.Heading.Ingress)
                {
                    if (CcCollective.IngressCount.ZeroNext(CcCollective.parm_max_inbound) >=
                        CcCollective.parm_max_inbound)
                    {
                        _logger.Trace($"{nameof(AttachDrone)}: {Direction} [FULL!]; {ccDrone.Description}");
                        return false;
                    }
                }
                else
                {
                    if (CcCollective.EgressCount.ZeroNext(CcCollective.parm_max_outbound) >=
                        CcCollective.parm_max_outbound)
                    {
                        _logger.Trace($"{nameof(AttachDrone)}: {Direction} [FULL!]; {ccDrone.Description}");
                        return false;
                    }
                }

                _logger.Trace($"{nameof(AttachDrone)}: [WON] {ccDrone?.Description}");

                Interlocked.Exchange(ref _drone, ccDrone);
                Assimilated = true;
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                //Scan for more...
                IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                {
                    var @this = (CcAdjunct)state;
                    await Task.Delay(@this.parm_web_settle_ms);
                    if (!((IoNanoprobe)state).Zeroed())
                        await ((CcAdjunct)state).ScanAsync().FastPath();
                }, this);

                return success = true;
            }
            catch when (!Zeroed())
            {
            }
            catch (Exception e) when (Zeroed())
            {
                _logger.Trace(e, Description);
            }
            finally
            {
                if (!success)
                    Interlocked.Exchange(ref _direction, 0);
            }

            return false;
        }

        /// <summary>
        /// If a connection was ever made
        /// </summary>
        public bool WasAttached { get; protected internal set; }

        /// <summary>
        /// Detaches a drone from this neighbor
        /// </summary>
        public async ValueTask DetachDroneAsync()
        {
            var latch = _drone;
            CcDrone severedDrone;
            if(latch == null || (severedDrone = Interlocked.CompareExchange(ref _drone, null, latch)) != latch)
                return;

            try
            {
                AttachTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                Interlocked.Exchange(ref _direction, 0);

                Interlocked.Exchange(ref _connectionAttempts, 0);
                Interlocked.Exchange(ref _openSlots, 0);
                Interlocked.Exchange(ref _scanCount, 0);
                //Interlocked.Exchange(ref _probed, 0);

                if (!Zeroed())
                {
                    //back off for a while... Try to re-establish a link 
                    IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
                    {
                        var @this = (CcAdjunct)state;

                        if (@this.RetryOnDc)
                        {
                            await @this.BackOffAsync;
                            var router = !@this.Zeroed() ? @this : @this.Router;
                            await router.ProbeAsync("SYN-RE", @this.Dmz, @this.Designation.IdString()).FastPath();
                        }
                    }, this);
                }

                //emit event

                try
                {
                    if (!CcCollective.ZeroDrone && AutoPeeringEventService.Operational && CcCollective != null)
                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                        {
                            EventType = AutoPeerEventType.RemoveDrone,
                            Drone = new Drone
                            {
                                CollectiveId = CcCollective.CcId.IdString(),
                                Id = Designation.IdString()
                            }
                        });
                }
                catch
                {
                    // ignored
                }

                CcCollective.Adapt();
            }
            catch when (Zeroed())
            { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(DetachDroneAsync)}: ");
            }
            finally
            {
                ResetState(AdjunctState.Verified);
                await severedDrone.DisposeAsync(this, $"detached from adjunct, was attached = {WasAttached}").FastPath();
                if(WasAttached)
                    await DeFuseAsync().FastPath();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AdjunctState ResetState(AdjunctState state)
        {
            Interlocked.Exchange(ref _direction, 0);
            Interlocked.Exchange(ref _openSlots, 0);
            Interlocked.Exchange(ref _connectionAttempts, 0);

            _state.Set((int)state);

            return _state.Value;
        }

        /// <summary>
        /// Makes fusing more likely
        /// </summary>
        public void EnsureFuseChecks()
        {
            if (!IsDroneAttached)
            {
                Interlocked.Exchange(ref _connectionAttempts, 0);
                Interlocked.Exchange(ref _scanCount, 0);
            }
        }

#if DEBUG
        [MethodImpl(MethodImplOptions.Synchronized)]
#else
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif

        public AdjunctState CompareAndEnterState(AdjunctState state, AdjunctState cmp, bool compare = true, int overrideHung = 0)
        {
            var oldValue = _state.Value;
#if DEBUG
            try
            {
                //TODO Make HEAP
                var nextState = new IoStateTransition<AdjunctState>((int)state);
                
                //force hung states
                if (overrideHung > 0)
                {
                    var hung = _state.EnterTime.ElapsedMs() > overrideHung;
                    if (hung)
                    {
                        oldValue = cmp;
                        Interlocked.Exchange(ref _direction, 0);
                        Interlocked.Exchange(ref _openSlots, 0);
                        Interlocked.Exchange(ref _connectionAttempts, 0);
                    }

                    compare = compare && !hung;
                }

                if (compare)
                {
#if DEBUG
                    if (oldValue != cmp)
                        return oldValue;
#else
                    return _state.CompareAndEnterState((int)state, (int)cmp);
#endif
                }
#if !DEBUG
                else
                {
                    _state.Set((int)state);
                    return oldValue;
                }
#endif
#if DEBUG
                Interlocked.Exchange(ref _state, _state.Exit(nextState));
#endif
            }
            catch when(!Zeroed()){}
            catch (Exception e)when(Zeroed())
            {
                _logger.Error(e, $"{nameof(CompareAndEnterState)} failed!");
            }
#else
            if (overrideHung > 0)
            {
                var hung = _state.EnterTime.ElapsedMs() > overrideHung;

                if (hung)
                {
                    oldValue = cmp;
                    _connectionAttempts = _scanCount = 0;
                    compare = false;
                }
            }

            if (compare)
            {
                return _state.CompareAndEnterState((int)state, (int)cmp);
            }
            else
            {
                _state.Set((int)state);
            }
#endif
            return oldValue;
        }

        /// <summary>
        /// Print state history
        /// </summary>
#if DEBUG
        public void PrintStateHistory()
#else
        public static void PrintStateHistory()
#endif
        {
#if DEBUG
            var ioStateTransition = _state.GetStartState();
            var sb = new StringBuilder();

            while (ioStateTransition != null)
            {
                sb.Append($"{ioStateTransition.Value} ~> ");
                ioStateTransition = ioStateTransition.Next;
            }

            if(sb.Length > 0)
                LogManager.GetCurrentClassLogger().Fatal(sb.ToString()[..(sb.Length-3)]);
#else
            //return default;
#endif
        }

        public IoStateTransition<AdjunctState> CurrentState => _state;

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public AdjunctState State => _state.Value;

        /// <summary>
        /// Don't retry on destruction
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CcAdjunct NoRetry()
        {
            Volatile.Write(ref _retryOnDc, false);
            return this;
        }
    }
}