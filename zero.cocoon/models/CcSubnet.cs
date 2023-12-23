#define DUPCHECK
using K4os.Compression.LZ4;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Google.Protobuf;
using Org.BouncyCastle.Crypto.Engines;
using sabot;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.feat.models.protobuffer;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.runtime.scheduler;
using Zero.Models.Protobuf;
using zero.core.patterns.heap;

namespace zero.cocoon.models
{
    public class CcSubnet: CcProtocMessage<CcWhisperMsg, CcGossipBatch>
    {
        static CcSubnet()
        {
            SendBuf = new IoHeap<byte[]>($"{nameof(CcSubnet)}:", 8192, (_, _) => new byte[32], true);
        }

        public CcSubnet(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            
        }

        private const int sabotSize = 32;
        private readonly byte[] _idBuffer = new byte[sabotSize];

        public override long Signature { get ; protected set; }
        private long Challenge => MemoryMarshal.Read<long>(_idBuffer);


        private static readonly IoHeap<byte[]> SendBuf;
        //private static readonly IoHeap<Tuple<BrotliStream, byte[]>> _sendBuffer;
        private readonly byte[] _vb = new byte[sizeof(ulong)];

        private CcWhisperMsg _protoBuf;

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

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //are we in recovery mode?
            var zeroRecovery = State == IoJobMeta.JobState.ZeroRecovery;

            var fastPath = false;
            try
            {
                //fail fast
                if (BytesRead == 0)
                    return await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();

                //Ensure that the previous job (which could have been launched out of sync) has completed
                var prevJob = ((CcSubnet)PreviousJob)?.ZeroRecovery;
                if (prevJob != null)
                {
                    fastPath = IoZero.ZeroRecoveryEnabled && prevJob.GetStatus() == ValueTaskSourceStatus.Succeeded && prevJob.GetResult(0);
                    if (zeroRecovery || fastPath)
                    {
                        if (fastPath)
                        {
                            await SetStateAsync(IoJobMeta.JobState.ZeroRecovery).FastPath();
                            await AddRecoveryBitsAsync().FastPath();
                        }
                        else
                        {
                            var prevJobTask = new ValueTask<bool>(prevJob, 0);
                            if (await prevJobTask.FastPath())
                                await AddRecoveryBitsAsync().FastPath();
                        }
                    }
                }

                while (BytesLeftToProcess > 0)
                {
                    var read = 0;
                    //deserialize
                    try
                    {
                        int length;
                        while ((length = ZeroSync()) > 0)
                        {
                            //Console.WriteLine($"bs - in {length} bytes - {ReadOnlyMemory.Slice(BufferOffset + sizeof(ulong),length).PayloadSig()}");
                            if (length > BytesLeftToProcess)
                            {
                                await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
                                break;
                            }
                            try
                            {
                                var time = MemoryMarshal.Read<long>(MemoryBuffer[(BufferOffset + sizeof(int) + 2 * sizeof(long))..].Span);
                                if (time.ElapsedUtcMs() > CcCollective.parm_time_e * 1000 * 2)
                                    goto Skip;

                                var tick = MemoryMarshal.Read<int>(MemoryBuffer[(BufferOffset + 2 * sizeof(long))..].Span);
                                if (!CcDrone.ZeroSynced(tick))
                                    goto Skip;

                                Signature = MemoryMarshal.Read<long>(MemoryBuffer[(BufferOffset + sizeof(long))..].Span);
#if DUPCHECK

                                if (!DupCheck())
#endif
                                {
                                    Sabot.ComputeHash(MemoryBuffer.Span, BufferOffset + sizeof(int) + sizeof(long), length, _idBuffer);

                                    if (Signature == Challenge)
                                    {
                                        var unpackOffset = DatumProvisionLengthMax << 2;
                                        var packetLen = LZ4Codec.Decode(Buffer, BufferOffset + sizeof(int) + 2 * sizeof(long), length,
                                            Buffer, unpackOffset, Buffer.Length - unpackOffset - 1);

                                        if (packetLen > 0)
                                            _protoBuf = CcWhisperMsg.Parser.ParseFrom(Buffer, unpackOffset, packetLen);
                                    }
                                    else
                                    {
                                        await SetStateAsync(IoJobMeta.JobState.ZeroSec).FastPath();
                                        await SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();
                                    }
                                }

                                Skip:
                                read = length + sizeof(int) + 2 * sizeof(long);
                                Interlocked.Add(ref BufferOffset, read);

                                if (State == IoJobMeta.JobState.ZeroRecovery)
                                {
                                    await SetStateAsync(IoJobMeta.JobState.Synced).FastPath();
                                    await SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();
                                }

                                break;
                            }
#if DEBUG
                            catch (Exception e)
                            {
                                await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                                _logger.Trace(e,
                                    $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, {Description}");
                            }
#else
                            catch
                            {
                                await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                                //_logger.Trace(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess }/{BytesRead}/{BytesLeftToProcess }, d = {DatumCount}, syncing = {InRecovery}, {Description}");
                            }
#endif
                        }

                        if (read == 0 && length == -1)
                        {
                            await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
                            break;
                        }
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                        _logger.Debug(e, $"Parse failed: buf[{BufferOffset}], r = {BytesRead - BytesLeftToProcess}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, {Description}");
                        break;
                    }

                    if (State != IoJobMeta.JobState.Consuming)
                        break;

                    //Sanity check the data
                    if (_protoBuf == null || _protoBuf.Data == null || _protoBuf.Data.Length == 0 || !CcDrone.AccountingBit)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                        continue;
                    }

                    //entropy
                    IoZero.IoSource.ZeroTimeStamp = Environment.TickCount;
                    IoZero.IncEventCounter(); 
                    CcCollective.BroadcastReceived(_protoBuf);

                    //broadcast not seen
#if THROTTLE
                    //if (req > CcCollective.parm_max_drone && Source.Rate.ElapsedMs() < HeartbeatTime) //TODO: tuning, network tick rate of 2 per second
                    //    continue;
#endif


                    //if (Source.SetRateSet(1, 0) != 0)
                    //    continue;

                    //Console.WriteLine($"req -> {req}");


                    IoZeroScheduler.Zero.QueueAsyncFunction(static state =>
                    {
                        var (@this,broadcastMsg) = (ValueTuple<CcSubnet, CcWhisperMsg>)state;

                        if ( @this.Zeroed() )
                            return new ValueTask(Task.CompletedTask);
#if THROTTLE
                        //await Task.Delay(RandomNumberGenerator.GetInt32(HeartbeatTime/2, HeartbeatTime));
                        //await Task.Delay(30);
#endif
                        byte[] socketBuf = null;
                        var processed = 0;
                        try
                        {
                            var fanOut = @this.CcCollective.Neighbors.Values
                                .Where(d => ((CcDrone)d).MessageService?.IsOperational() ?? false).ToList();

                            if ((processed = fanOut.Count) == 0)
                                return default;

                            @this.CcCollective.DupChecker.TryGetValue(@this.Signature, out var dupEndpoints);

                            foreach (var fanDrone in fanOut)
                            {
                                var drone = (CcDrone)fanDrone;
                                try
                                {
                                    var source = drone.MessageService;
                                    if (source == null || source.Zeroed())
                                    {
                                        processed--;
                                        continue;
                                    }

#if DUPCHECK
                                    //Don't forward new messages to nodes from which we have received the msg in the mean time.
                                    //This trick has the added bonus of using congestion as a governor to catch more of those overlaps, 
                                    //which in turn lowers the traffic causing less congestion

                                    var dup = false;
                                    foreach (var dupEndpoint in dupEndpoints!)
                                    {
                                        if (dupEndpoint.Source == source.IoNetSocket.Key)
                                        {
                                            dup = true;
                                            Interlocked.Increment(ref dupEndpoint.Count);
                                            break;
                                        }
                                    }

                                    if (dup)
                                    {
                                        processed--;
                                        continue;
                                    }
#endif
                                    IoZeroScheduler.Zero.QueueAsyncFunction(static async context =>
                                    {
                                        var (@this, drone, dupEndpoints, source, broadcastMsg, collect) = (ValueTuple<CcSubnet, CcDrone, List<string>, IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>, CcWhisperMsg, bool>)context;

                                        //update latest state
                                        try
                                        {
                                            //await Task.Delay(RandomNumberGenerator.GetInt32(500,1500));
                                            //await Task.Delay(1);

                                            if (dupEndpoints?.Contains(source.IoNetSocket.Key) ?? false)
                                                return;

                                            var sent = 0;
                                            if (source.IoNetSocket == null ||
                                                (sent = await source.IoNetSocket.SendAsync(broadcastMsg.Data.Memory, 0, broadcastMsg.Data.Length).FastPath()) <= 0)
                                            {
                                                _logger.Trace($"SendAsync: FAILED; sent = {sent}, {source?.Description}");
                                                return;
                                            }

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
                                        catch when (@this.Zeroed()) { }
                                        catch (Exception e) when (!@this.Zeroed())
                                        {
                                            _logger.Error(e, $"{nameof(source.IoNetSocket.SendAsync)}: Fanout!");
                                        }
                                        //finally
                                        //{
                                        //    if (collect)
                                        //    {

                                        //    }
                                        //}
                                    }, (@this, drone, dupEndpoints, source, broadcastMsg, --processed == 0));
                                    //if (source == null || await source.IoNetSocket
                                    //        .SendAsync(socketBuf, 0, (int)(compressed + sizeof(ulong)))
                                    //        .FastPath() <= 0)
                                    //{
                                    //    _logger.Trace($"SendAsync: FAILED; {req} {source?.Description}");
                                    //    continue;
                                    //}

                                    //if (source == null || await source.IoNetSocket.SendAsync(socketXBuf.Item2, 0, (int)socketXBuf.Item1.BaseStream.Position + sizeof(ulong)).FastPath() <= 0) continue;
                                    //if (AutoPeeringEventService.Operational)
                                    //    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                                    //    {
                                    //        EventType = AutoPeerEventType.SendProtoMsg,
                                    //        Msg = new ProtoMsg
                                    //        {
                                    //            CollectiveId = @this.CcCollective.Hub.Router.Designation.IdString(),
                                    //            Id = drone.Adjunct.Designation.IdString(),
                                    //            Type = $"gossip{@this.Id % 6}"
                                    //        }
                                    //    });

#if THROTTLE
                                    //await Task.Delay(30);

                                    if (@this.Source.Rate > 0 && @this.Source.Rate.ElapsedMs() < HeartbeatTime) //TODO: tuning, network tick rate of 2 per second
                                        break;
#endif
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

                            //@this.Source.SetRate(Environment.TickCount, @this.Source.Rate);
                        }
                        catch when (@this.Zeroed())
                        {
                        }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            IoJob<CcSubnet>._logger.Error(e, $"{@this.Description}");
                        }
                        finally
                        {
                            if (processed != 0)
                                SendBuf.Return(socketBuf);
                        }

                        return new ValueTask(Task.CompletedTask);
                    }, (this, _protoBuf));
                }

                ////TODO tuning
                //if (_currentBatch.Count > _batchHeap.Capacity * 3 / 2)
                //    _logger.Warn($"{nameof(_batchHeap)} running lean {_currentBatch.Count}/{_batchHeap.Capacity}, {_batchHeap}, {_batchHeap.Description}");
                ////Release a waiter
                //await ZeroBatchAsync();
            }
            catch when (Zeroed()) { await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath(); }
            catch (Exception e) when (!Zeroed())
            {
                await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                _logger.Error(e, $"Unmarshal chroniton failed in {Description}");
            }
            finally
            {
                try
                {
                    if (BytesLeftToProcess == 0 && State == IoJobMeta.JobState.Consuming)
                        await SetStateAsync(IoJobMeta.JobState.Consumed).FastPath();
                    else if (BytesLeftToProcess != BytesRead)
                        await SetStateAsync(IoJobMeta.JobState.Fragmented).FastPath();
#if DEBUG
                    //else if (State == IoJobMeta.JobState.Fragmented)
                    //{
                    //    _logger.Debug($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                    //}
                    ////attempt zero recovery
                    if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !zeroRecovery && !fastPath && BytesLeftToProcess > 0 && PreviousJob != null)
                    {
                        await SetStateAsync(IoJobMeta.JobState.ZeroRecovery).FastPath();
                        await SetStateAsync(await ConsumeAsync().FastPath()).FastPath();
                    }

                    //else
                    //{
                    //    _logger.Fatal($"[{Id}] FRAGGED = {DatumCount}, {BytesRead}/{BytesLeftToProcess }");
                    //}
#else
                    else if (State == IoJobMeta.JobState.Fragmented && !IoZero.ZeroRecoveryEnabled)
                    {
                        await SetStateAsync(IoJobMeta.JobState.BadData).FastPath();
                    }
#endif
                    if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !zeroRecovery && !fastPath && BytesLeftToProcess > 0 && PreviousJob != null)
                    {
                        await SetStateAsync(IoJobMeta.JobState.ZeroRecovery).FastPath();
                        await SetStateAsync(await ConsumeAsync().FastPath()).FastPath();
                    }
                }
                catch when (Zeroed()) { await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath(); }
                catch (Exception e) when (!Zeroed())
                {
                    await SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
                    _logger.Error(e, $"{nameof(State)}: re setting state failed!");
                }
            }

            //////attempt zero recovery
            if (IoZero.ZeroRecoveryEnabled && !Zeroed() && !zeroRecovery && !fastPath && BytesLeftToProcess > 0 && PreviousJob != null)
            {
                await SetStateAsync(IoJobMeta.JobState.ZeroRecovery).FastPath();
                return await ConsumeAsync().FastPath();
            }

            return State;
        }

        private bool DupCheck()
        {
            //set this message as seen if seen before
            var endpoint = ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)Source).IoNetSocket.Key;

            if (!CcCollective.DupChecker.TryGetValue(Signature, out var dupEndpoints))
            {
                dupEndpoints = CcCollective.DupHeap.Take(endpoint);
                
                if (dupEndpoints == null)
                    throw new OutOfMemoryException(
                        $"{CcCollective.DupHeap}: {CcCollective.DupHeap.ReferenceCount}/{CcCollective.DupHeap.Capacity} - c = {CcCollective.DupChecker.Count}, m = _maxReq");

                if (!CcCollective.DupChecker.TryAdd(Signature, dupEndpoints))
                {
                    CcCollective.DupHeap.Return(dupEndpoints);

                    //best effort
                    if (CcCollective.DupChecker.TryGetValue(Signature, out dupEndpoints))
                    {
                        dupEndpoints.Add(CcCollective.RadarHeap.Take(endpoint));
                    }
                }
            }
            else
            {
                dupEndpoints.Add(CcCollective.RadarHeap.Take(endpoint));
            }

            return false;
        }
    }
}
