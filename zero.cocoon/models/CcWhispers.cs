using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Google.Protobuf;
using K4os.Compression.LZ4;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.feat.models.protobuffer;
using zero.core.misc;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;

namespace zero.cocoon.models
{
    public class CcWhispers : CcProtocMessage<CcWhisperMsg, CcGossipBatch>
    {
        public CcWhispers(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            //sentinel
            if (Source == null)
                return;

            _m = new CcWhisperMsg() { Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(_vb)) };
            _sendBuf = new IoHeap<byte[]>($"{nameof(_sendBuf)}: {Description}", 8, (_, _) => new byte[32], true);
        }

        private IoHeap<byte[]> _sendBuf;
        private readonly byte[] _vb = new byte[sizeof(ulong)];
        private readonly CcWhisperMsg _m;

        /// <summary>
        /// Zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            await _sendBuf.ZeroManagedAsync<object>().FastPath();
            //if (_protocolMsgBatch != null)
            //    _arrayPool.ReturnAsync(_protocolMsgBatch, true);

            //_batchMsgHeap.ZeroManaged(batchMsg =>
            //{
            //    batchMsg.ZeroAsync = null;
            //    batchMsg.Message = null;
            //    batchMsg.RemoteEndPoint = null;
            //});

            //await _dupHeap.ClearAsync();

        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            //_dupHeap = null;
            //_arrayPool = null;
            //_batchMsgHeap = null;
            _sendBuf = null;
            base.ZeroUnmanaged();
        }

        /// <summary>
        /// Zeroed?
        /// </summary>
        /// <returns>True of false</returns>
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed();
        }

        ///// <summary>
        ///// Batch of messages
        ///// </summary>
        //private volatile CcGossipBatch[] _protocolMsgBatch;

        ///// <summary>
        ///// Batch heap items
        ///// </summary>
        //private IoHeap<CcGossipBatch> _batchMsgHeap;


        /// <summary>
        /// The drone this whisper belongs too
        /// </summary>
        private CcDrone CcDrone => (CcDrone)IoZero;

        /// <summary>
        /// The adjunct this job belongs too
        /// </summary>
        private CcAdjunct CcAdjunct => CcDrone.Adjunct;

        /// <summary>
        /// The node this job belongs too
        /// </summary>
        private CcCollective CcCollective => CcAdjunct.CcCollective;

        ///// <summary>
        ///// Cc Identity
        ///// </summary>
        public CcDesignation CcId => CcCollective.CcId;

        /// <summary>
        /// random number generator
        /// </summary>
        //readonly Random _random = new Random((int)DateTime.Now.Ticks);

        //private IoHeap<ConcurrentBag<string>> _dupHeap;
        //private uint _poolSize = 50;
        //private long _maxReq = int.MinValue;

        /*public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            var read = 0;
            try
            {
                //fail fast
                if (BytesRead == 0 || Zeroed())
                {
                    return State = IoJobMeta.JobState.ConInlined;
                }

                var round = 0;
                while (BytesLeftToProcess > 0 && State == IoJobMeta.JobState.Consuming)
                {
                    CcWhisperMsg whispers = null;                    
                        
                    //deserialize
                    try
                    {
                        read = 10;
                        whispers = CcWhisperMsg.Parser.ParseFrom(ReadOnlySequence.Slice(BufferOffset, 10));
                        //read = whispers.CalculateSize();
                    }
                    catch when(!Zeroed())
                    {
                        State = IoJobMeta.JobState.ConsumeErr;
                        continue;
                    }
                    finally
                    {
                        Interlocked.Add(ref BufferOffset, read);
                    }

                    //if (read == 0)
                    //{
                    //    break;
                    //}

                    //Sanity check the data
                    if (whispers.Data == null || whispers.Data.Length == 0)
                    {
                        continue;
                    }                    
                    
                    //read the token
                    var req = MemoryMarshal.Read<long>(whispers.Data.Span);
#if DUPCHECK
                    //dup check memory management
                    try
                    {
                        if (!await CcCollective.DupSyncRoot.WaitAsync().FastPath())
                        {
                            State = IoJobMeta.JobState.ConsumeErr;
                            break;
                        }

                        if (CcCollective.DupChecker.Count > CcCollective.DupHeap.Capacity*4/5)
                        {
                            var culled = CcCollective.DupChecker.Keys.Where(k => k < req - CcCollective.DupPoolFPSTarget / 3).ToList();
                            foreach (var mId in culled)
                            {
                                if (CcCollective.DupChecker.TryRemove(mId, out var del))
                                {
                                    del.ZeroManaged();
                                    CcCollective.DupHeap.Return(del);
                                }
                            }
                        }
                    }
                    catch (Exception) when (Zeroed() || CcCollective == null || CcCollective.Zeroed() || CcCollective.DupSyncRoot == null)
                    {
                        State = IoJobMeta.JobState.ConsumeErr;
                    }
                    catch (Exception) when (!Zeroed() && CcCollective != null && !CcCollective.Zeroed() && CcCollective.DupSyncRoot != null)
                    {
                        State = IoJobMeta.JobState.ConsumeErr;
                    }
                    finally
                    {
                        try
                        {
                            if (CcCollective.DupSyncRoot.Release() == -1)
                            {
                                if (CcCollective != null && !CcCollective.DupSyncRoot.Zeroed() && !Zeroed())
                                {
                                    _logger.Error($"{Description}, rounds = {round}, drone = {CcDrone}, adjunct = {CcAdjunct}, cc = {CcCollective}, sync = `{CcCollective.DupSyncRoot}'");
                                    //PrintStateHistory();
                                    State = IoJobMeta.JobState.ConsumeErr;
                                }
                                else
                                    State = IoJobMeta.JobState.Cancelled;
                            }
                        }
                        catch (Exception) when (Zeroed() || CcCollective == null || CcCollective.Zeroed() ||CcCollective?.DupSyncRoot == null)
                        {
                            State = IoJobMeta.JobState.ConsumeErr;
                        }
                        catch (Exception e) when (!Zeroed() && CcCollective is { DupSyncRoot: { } } && !CcCollective.DupSyncRoot.Zeroed() && !CcCollective.Zeroed() )
                        {
                            _logger?.Fatal(e, $"{Description}, rounds = {round}, drone = {CcDrone}, adjunct = {CcDrone?.Adjunct}, cc = {CcDrone?.Adjunct?.CcCollective}, sync = `{CcDrone?.Adjunct?.CcCollective?.DupSyncRoot}'");
                            //PrintStateHistory();
                            State = IoJobMeta.JobState.ConsumeErr;
                        }
                    }

                    if (State != IoJobMeta.JobState.Consuming)
                        break;
                                        
                    //set this message as seen if seen before
                    var endpoint = ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)Source).IoNetSocket.RemoteAddress;
                    
                    if (!CcCollective.DupChecker.TryGetValue(req, out var dupEndpoints))
                    {
                        dupEndpoints = CcCollective.DupHeap.Take(endpoint);

                        if (dupEndpoints == null)
                            throw new OutOfMemoryException(
                                $"{CcCollective.DupHeap}: {CcCollective.DupHeap.ReferenceCount}/{CcCollective.DupHeap.Capacity} - c = {CcCollective.DupChecker.Count}, m = _maxReq");
                        
                        if (!CcCollective.DupChecker.TryAdd(req, dupEndpoints))
                        {
                            dupEndpoints.ZeroManaged();
                            CcCollective.DupHeap.Return(dupEndpoints);

                            //best effort
                            if (CcCollective.DupChecker.TryGetValue(req, out dupEndpoints))
                                dupEndpoints.Add(endpoint.GetHashCode(), true);
                            
                            continue;
                        }                        
                    }
                    else
                    {
                        dupEndpoints.Add(endpoint.GetHashCode(), true);
                        continue;
                    }
#endif
                    //await Task.Delay(1000);
                    //Console.WriteLine(".");
                    IoZero.IncEventCounter();
                    CcCollective.IncEventCounter();

                    req++;
                    MemoryMarshal.Write(Buffer[(BufferOffset - read)..], ref req);

                    if (req % 100000 == 0)
                    {
                        _logger.Info($"{Id}");
                    }

                    foreach (var drone in CcCollective.WhisperingDrones)
                    {
                        try
                        {
                            var source = (IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)drone.Source;

                            //Don't forward new messages to nodes from which we have received the msg in the mean time.
                            //This trick has the added bonus of using congestion as a governor to catch more of those overlaps, 
                            //which in turn lowers the traffic causing less congestion
#if DUPCHECK
                            if (source.IoNetSocket.RemoteAddress == endpoint || dupEndpoints != null && dupEndpoints.Contains(source.IoNetSocket.RemoteAddress.GetHashCode()))
                                continue;
#endif
                            if (await source.IsOperational().FastPath() && await source.IoNetSocket.SendAsync(Buffer, BufferOffset - read, read).FastPath() <= 0)
                            {
                                if(await source.IsOperational().FastPath())
                                    _logger.Trace($"Failed to forward new msg message to {drone.Description}");
                            }
                            else
                            {
                                if (AutoPeeringEventService.Operational)
                                    await AutoPeeringEventService.AddEvent(new AutoPeerEvent
                                    {
                                        EventType = AutoPeerEventType.SendProtoMsg,
                                        Msg = new ProtoMsg
                                        {
                                            CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                                            Id = drone.Adjunct.Designation.IdString(),
                                            Type = $"gossip{Id % 6}"
                                        }
                                    });
                            }
                        }
                        catch when(Zeroed() || CcCollective == null || CcCollective.Zeroed() || CcCollective.DupSyncRoot == null) {}
                        catch (Exception e)when(!Zeroed())
                        {
                            _logger.Trace(e, Description);
                        }
                    }
                }

                State = IoJobMeta.JobState.Consumed;
            }
            catch when(Zeroed() || CcCollective == null || CcCollective.Zeroed() || CcCollective.DupSyncRoot == null) {}
            //catch(NullReferenceException){}//TODO
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"Unmarshal chroniton failed in {Description}");
            }
            finally
            {
                if (State == IoJobMeta.JobState.Consuming)
                    State = IoJobMeta.JobState.ConsumeErr;
            }

            return State;
        }*/

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //are we in recovery mode?
            var zeroRecovery = State == IoJobMeta.JobState.ZeroRecovery;
            
            var fastPath = false;
            try
            {
                //fail fast
                if (BytesRead == 0)
                    return await SetState(IoJobMeta.JobState.BadData).FastPath();

                //Ensure that the previous job (which could have been launched out of sync) has completed
                var prevJob = ((CcWhispers)PreviousJob)?.ZeroRecovery;
                if (prevJob != null)
                {
                    fastPath = IoZero.ZeroRecoveryEnabled && prevJob.GetStatus((short)prevJob.Version) == ValueTaskSourceStatus.Succeeded && prevJob.GetResult((short)prevJob.Version);
                    if (zeroRecovery || fastPath)
                    {
                        if (fastPath)
                        {
                            await SetState(IoJobMeta.JobState.ZeroRecovery).FastPath();
                            await AddRecoveryBits().FastPath();
                        }
                        else
                        {
                            var prevJobTask = new ValueTask<bool>(prevJob, (short)prevJob.Version);
                            if (await prevJobTask.FastPath())
                                await AddRecoveryBits().FastPath();
                        }
                    }
                }
                
                while (BytesLeftToProcess > 0)
                {
                    CcWhisperMsg packet = null;

                    var read = 0;
                    //deserialize
                    try
                    {
                        int length;
                        while ((length = ZeroSync()) > 0)
                        {
                            if (length > BytesLeftToProcess)
                            {
                                await SetState(IoJobMeta.JobState.Fragmented).FastPath();
                                break;
                            }

                            try
                            {
                                var unpackOffset = DatumProvisionLengthMax << 2;
                                var packetLen = LZ4Codec.Decode(Buffer, BufferOffset + sizeof(ulong), length, Buffer, unpackOffset, Buffer.Length - unpackOffset - 1);

                                read = length + sizeof(ulong);
                                Interlocked.Add(ref BufferOffset, read);

                                if (packetLen > 0)
                                    packet = CcWhisperMsg.Parser.ParseFrom(Buffer, unpackOffset, packetLen);

                                if (State == IoJobMeta.JobState.ZeroRecovery)
                                {
                                    await SetState(IoJobMeta.JobState.Synced).FastPath();
                                    await SetState(IoJobMeta.JobState.Consuming).FastPath();
                                }
                                break;
                            }
#if DEBUG
                            catch (Exception e)
                            {
                                await SetState(IoJobMeta.JobState.BadData).FastPath();
                                _logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, {Description}");
                            }
#else
                            catch
                            {
                                await SetState(IoJobMeta.JobState.BadData).FastPath();
                                //_logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, syncing = {InRecovery}, {Description}");
                            }
#endif
                        }

                        if (read == 0 && length == -1)
                        {
                            await SetState(IoJobMeta.JobState.Fragmented).FastPath();
                            break;
                        }
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        await SetState(IoJobMeta.JobState.ConsumeErr).FastPath();
                        _logger.Debug(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, {Description}");
                        break;
                    }

                    if (State != IoJobMeta.JobState.Consuming)
                        break;

                    //Sanity check the data
                    if (packet == null || packet.Data == null || packet.Data.Length == 0 || !CcDrone.AccountingBit)
                    {
                        await SetState(IoJobMeta.JobState.BadData).FastPath();
                        continue;
                    }

                    //if(Id % 5 != 0)
                    //await Task.Delay(1000/64);
                    //await Task.Delay(RandomNumberGenerator.GetInt32(0,2000));
                    //await Task.Delay(500);
                    //await Task.Delay(1000);
                    
                    //read the token
                    var req = MemoryMarshal.Read<long>(packet.Data.Span);

                    if (req < 0)
                        continue;

                    if (CcCollective.MaxReq > 1000 && req < 10)
                    {
                        Console.WriteLine($"RESET[{req}] {CcCollective.MaxReq} -> {IoZero.Description}");
                        CcCollective.MaxReq = -1;
                    }

                    if (req > 0 && req % 100000 == 0)
                    {
                        _logger.Info($"[{Id}]: {req}, recover = {Source.Counters[(int)IoJobMeta.JobState.Synced]}/{Source.Counters[(int)IoJobMeta.JobState.ZeroRecovery]} ({Source.Counters[(int)IoJobMeta.JobState.Synced]/(double)Source.Counters[(int)IoJobMeta.JobState.ZeroRecovery]*100:0.0}%), frag = {Source.Counters[(int)IoJobMeta.JobState.Fragmented]}, bad = {Source.Counters[(int)IoJobMeta.JobState.BadData]}, success = {Source.Counters[(int)IoJobMeta.JobState.Consumed]}, fail = {Source.Counters[(int)IoJobMeta.JobState.Queued] - Source.Counters[(int)IoJobMeta.JobState.Consumed]}");
                    }
                    req++;

                    if (req is > uint.MaxValue or < 0 || req <= Volatile.Read(ref CcCollective.MaxReq))
                    {
                        //Console.Write($". r = {req}, mr = {Volatile.Read(ref _maxReq)}");
                        continue;
                    }


                    if (req > 2 && Source.Rate.ElapsedMs() < 3000)// && req - _maxReq < 10)
                        continue;

                    //await Task.Delay(RandomNumberGenerator.GetInt32(5, 350));
                    //await Task.Delay(RandomNumberGenerator.GetInt32(16, 90));

                    var latch = Source.Rate;
                    if( Source.SetRate(Environment.TickCount, latch) != latch)
                        continue;
                    
                    IoZero.IncEventCounter();
                    CcCollective.IncEventCounter();

                    await ZeroAsync(static async state =>
                    {
                        var (@this, req) = state;
                        
                        if(req <= Volatile.Read(ref @this.CcCollective.MaxReq))
                            return;

                        await Task.Delay(RandomNumberGenerator.GetInt32(500, 1500));

                        if (req <= Volatile.Read(ref @this.CcCollective.MaxReq))
                            return;

                        var latch = @this.CcCollective.MaxReq;
                        if (Interlocked.CompareExchange(ref @this.CcCollective.MaxReq, req, @this.CcCollective.MaxReq) != latch)
                            return;

                        byte[] socketBuf = null;
                        try
                        {
                            socketBuf = @this._sendBuf.Take();
                            MemoryMarshal.Write(@this._vb.AsSpan(), ref req);

                            var protoBuf = @this._m.ToByteArray();
                            var compressed = (ulong)LZ4Codec.Encode(protoBuf, 0, protoBuf.Length, socketBuf,
                                sizeof(ulong), socketBuf.Length - sizeof(ulong));
                            MemoryMarshal.Write(socketBuf, ref compressed);

                            if (!@this.Zeroed())
                            {
                                foreach (var drone in @this.CcCollective.Neighbors.Values.Cast<CcDrone>().Where(d=>d.MessageService?.IsOperational()??false))
                                {
                                    if (req != Volatile.Read(ref @this.CcCollective.MaxReq))
                                        break;
                                    try
                                    {
                                        var source = drone.MessageService;

                                        //Don't forward new messages to nodes from which we have received the msg in the mean time.
                                        //This trick has the added bonus of using congestion as a governor to catch more of those overlaps, 
                                        //which in turn lowers the traffic causing less congestion
#if DUPCHECK
                            if (source.IoNetSocket.RemoteAddress == endpoint || dupEndpoints != null && dupEndpoints.Contains(source.IoNetSocket.RemoteAddress.GetHashCode()))
                                continue;
#endif
                                        if (source == null || await source.IoNetSocket.SendAsync(socketBuf, 0, (int)compressed + sizeof(ulong)).FastPath() <= 0) continue;
                                        {
                                            if (AutoPeeringEventService.Operational)
                                                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                                                {
                                                    EventType = AutoPeerEventType.SendProtoMsg,
                                                    Msg = new ProtoMsg
                                                    {
                                                        CollectiveId = @this.CcCollective.Hub.Router.Designation
                                                            .IdString(),
                                                        Id = drone.Adjunct.Designation.IdString(),
                                                        Type = $"gossip{@this.Id % 6}"
                                                    }
                                                });
                                        }
                                    }
                                    catch when (@this.Zeroed() || @this.CcCollective == null ||
                                                @this.CcCollective.Zeroed() || @this.CcCollective.DupSyncRoot == null)
                                    {
                                    }
                                    catch (Exception e) when (!@this.Zeroed())
                                    {
                                        _logger.Trace(e, @this.Description);
                                    }
                                }
                            }
                        }
                        catch when (@this.Zeroed())
                        {
                        }
                        catch (Exception e) when (@this.Zeroed())
                        {
                            IoJob<CcWhispers>._logger.Error(e,$"{@this.Description}");
                        }
                        finally
                        {
                            @this._sendBuf.Return(socketBuf);
                        }
                    }, (this, req), TaskCreationOptions.DenyChildAttach).FastPath();

                }

                ////TODO tuning
                //if (_currentBatch.Count > _batchHeap.Capacity * 3 / 2)
                //    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Count}/{_batchHeap.Capacity}, {_batchHeap}, {_batchHeap.Description}");
                ////Release a waiter
                //await ZeroBatchAsync();
            }
            catch when (Zeroed()) { await SetState(IoJobMeta.JobState.ConsumeErr).FastPath(); }
            catch (Exception e) when (!Zeroed())
            {
                await SetState(IoJobMeta.JobState.ConsumeErr).FastPath();
                _logger.Error(e, $"Unmarshal chroniton failed in {Description}");
            }
            finally
            {
                try
                {
                    if (BytesLeftToProcess == 0 && State == IoJobMeta.JobState.Consuming)
                        await SetState(IoJobMeta.JobState.Consumed).FastPath();
                    else if (BytesLeftToProcess != BytesRead)
                        await SetState(IoJobMeta.JobState.Fragmented).FastPath();
#if DEBUG
                    //else if (State == IoJobMeta.JobState.Fragmented)
                    //{
                    //    _logger.Debug($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                    //}
                    ////attempt zero recovery
                    if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !zeroRecovery && !fastPath && BytesLeftToProcess > 0 && PreviousJob != null)
                    {
                        await SetState(IoJobMeta.JobState.ZeroRecovery).FastPath();
                        await SetState(await ConsumeAsync().FastPath()).FastPath();
                    }

                    //else
                    //{
                    //    _logger.Fatal($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                    //}
#else
                    else if (State == IoJobMeta.JobState.Fragmented && !IoZero.ZeroRecoveryEnabled)
                    {
                        await SetState(IoJobMeta.JobState.BadData).FastPath();
                    }
#endif
                    if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !zeroRecovery && !fastPath && BytesLeftToProcess > 0 && PreviousJob != null)
                    {
                        await SetState(IoJobMeta.JobState.ZeroRecovery).FastPath();
                        await SetState(await ConsumeAsync().FastPath()).FastPath();
                    }
                }
                catch when (Zeroed()) { await SetState(IoJobMeta.JobState.ConsumeErr).FastPath(); }
                catch (Exception e) when (!Zeroed())
                {
                    await SetState(IoJobMeta.JobState.ConsumeErr).FastPath();
                    _logger.Error(e, $"{nameof(State)}: re setting state failed!");
                }
            }

            //////attempt zero recovery
            //if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !zeroRecovery && !fastPath && BytesLeftToProcess > 0 && PreviousJob != null)
            //{
            //    State = IoJobMeta.JobState.ZeroRecovery;
            //    return await ConsumeAsync();
            //}

            return State;
        }

        /// <summary>
        /// Processes a generic request
        /// </summary>
        /// <param name="packet">The packet</param>
        /// <typeparam name="T">The expected type</typeparam>
        /// <returns>The task</returns>
        //private ValueTask ProcessRequestAsync<T>(CcWhisperMsg packet)
        //    where T : IMessage<T>, IMessage, new()
        //{
        //    //try
        //    //{
        //    //    var parser = new MessageParser<T>(() => new T());
        //    //    var request = parser.ParseFrom(packet.Data);

        //    //    if (request != null)
        //    //    {
        //    //        //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );
        //    //        IIoZero zero = null;
        //    //        //if (((IoNetClient<CcPeerMessage>)Source).Socket.FfAddress != null)
        //    //        //    zero = IoZero;

        //    //        if (CurrBatchSlot >= parm_max_msg_batch_size)
        //    //            await ForwardToNeighborAsync().ConfigureAwait(ZC);

        //    //        var remoteEp = new IPEndPoint(((IPEndPoint)RemoteEndPoint).Address, ((IPEndPoint)RemoteEndPoint).Port);
        //    //        ProtocolMsgBatch[CurrBatchSlot] = ValueTuple.Create(zero, request, remoteEp, packet);
        //    //        Interlocked.Increment(ref CurrBatchSlot);
        //    //    }
        //    //}
        //    //catch (NullReferenceException e)
        //    //{
        //    //    _logger.Trace(e, Description);
        //    //}
        //    //catch (Exception e)
        //    //{
        //    //    _logger.Error(e,
        //    //        $"Unable to parse request type {typeof(T).Name} from {Convert.ToBase64String(packet.PublicKey.Memory.AsArray())}, size = {packet.Data.Length}");
        //    //}
        //    return default;
        //}

        /// <summary>
        /// Forward jobs to conduit
        /// </summary>
        /// <returns>Task</returns>
        //private async ValueTask ForwardToNeighborAsync()
        //{
        //    try
        //    {
        //        if (CurrBatchSlot == 0)
        //            return;

        //        if (CurrBatchSlot < parm_max_msg_batch_size)
        //        {
        //            _protocolMsgBatch[CurrBatchSlot] = default;
        //        }

        //        //cog the source
        //        var cogSuccess = await ProtocolConduit.Source.ProduceAsync(async (source, _, __, ioJob) =>
        //        {
        //            var _this = (CcWhispers)ioJob;

        //            if (!await ((CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>)source).PushBackAsync(_this._protocolMsgBatch).ConfigureAwait(ZC))
        //            {
        //                if (!((CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>)source).Zeroed())
        //                    _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {_this.Description}");
        //                return false;
        //            }

        //            //Retrieve batch buffer
        //            try
        //            {
        //                _this._protocolMsgBatch = _arrayPool.Rent(_this.parm_max_msg_batch_size);
        //            }
        //            catch (Exception e)
        //            {
        //                _logger.Fatal(e, $"Unable to rent from mempool: {_this.Description}");
        //                return false;
        //            }

        //            _this.CurrBatchSlot = 0;

        //            return true;
        //        }, jobClosure: this).ConfigureAwait(ZC);

        //        ////forward transactions
        //        // if (cogSuccess)
        //        // {
        //        //     if (!await ProtocolConduit.ProduceAsync().ConfigureAwait(ZC))
        //        //     {
        //        //         _logger.Warn($"{TraceDescription} Failed to forward to `{ProtocolConduit.Source.Description}'");
        //        //     }
        //        // }
        //    }
        //    catch (TaskCanceledException e)
        //    {
        //        _logger.Trace(e, Description);
        //    }
        //    catch (OperationCanceledException e)
        //    {
        //        _logger.Trace(e, Description);
        //    }
        //    catch (ObjectDisposedException e)
        //    {
        //        _logger.Trace(e, Description);
        //    }
        //    catch (NullReferenceException e)
        //    {
        //        _logger.Trace(e, Description);
        //    }
        //    catch (Exception e)
        //    {
        //        _logger.Debug(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
        //    }
        //}
    }
}
