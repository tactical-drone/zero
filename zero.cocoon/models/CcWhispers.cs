using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Proto;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.core;
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.models.protobuffer.sources;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.cocoon.models
{
    public class CcWhispers : CcProtocMessage<CcWhisperMsg, CcGossipBatch>
    {
        public CcWhispers(string sinkDesc, string jobDesc, IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            _dupHeap = new IoHeap<ConcurrentBag<string>>(_poolSize * 2)
            {
                Make = o => new ConcurrentBag<string>(),
                Prep = (popped, endpoint) =>
                {
                    popped.Add((string) endpoint);
                }
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override ValueTask ZeroManagedAsync()
        {
            _dupHeap.Clear();
            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            _dupHeap = null;
            base.ZeroUnmanaged();
        }

        //public override async ValueTask<bool> ConstructAsync()
        //{
        //    if (!MessageService.ObjectStorage.ContainsKey($"{nameof(CcProtocMessage<CcWhisperMsg, CcGossipBatch>)}.Gossip"))
        //    {
        //        CcProtocBatchSource<CcWhisperMsg, CcGossipBatch> channelSource = null;

        //        //Transfer ownership
        //        if (await MessageService.ZeroAtomicAsync((s, u, d) =>
        //        {
        //            channelSource = new CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>(MessageService, _arrayPool, 0, Source.ConcurrencyLevel * 2);
        //            if (MessageService.ObjectStorage.TryAdd($"{nameof(CcProtocMessage<CcWhisperMsg, CcGossipBatch>)}.Gossip", channelSource))
        //            {
        //                return ValueTask.FromResult(MessageService.ZeroOnCascade(channelSource).success);
        //            }

        //            return ValueTask.FromResult(false);
        //        }).ConfigureAwait(false))
        //        {
        //            ProtocolConduit = await MessageService.AttachConduitAsync(
        //                nameof(CcDrone),
        //                true,
        //                channelSource,
        //                userData => new CcProtocBatch<CcWhisperMsg, CcGossipBatch>(channelSource, -1 /*We block to control congestion*/),
        //                Source.ConcurrencyLevel * 2, Source.ConcurrencyLevel * 2
        //            ).ConfigureAwait(false);

        //            //get reference to a central mem pool
        //            if (ProtocolConduit != null)
        //                _arrayPool = ((CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>)ProtocolConduit.Source).ArrayPool;
        //            else
        //                return false;
        //        }
        //        else
        //        {
        //            var t = channelSource.ZeroAsync(new IoNanoprobe("Lost race on creation"));
        //            ProtocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<CcWhisperMsg, CcGossipBatch>>(nameof(CcDrone)).ConfigureAwait(false);
        //        }
        //    }
        //    else
        //    {
        //        ProtocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<CcWhisperMsg, CcGossipBatch>>(nameof(CcDrone)).ConfigureAwait(false);
        //        return ProtocolConduit != null;
        //    }

        //    return await base.ConstructAsync().ConfigureAwait(false);
        //}
        
        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile CcGossipBatch[] _protocolMsgBatch;

        
        /// <summary>
        /// CC Node
        /// </summary>
        private CcCollective CcCollective => ((CcDrone)IoZero).Adjunct.CcCollective;

        ///// <summary>
        ///// Cc Identity
        ///// </summary>
        public CcDesignation CcId => CcCollective.CcId;

        /// <summary>
        /// random number generator
        /// </summary>
        readonly Random _random = new Random((int)DateTime.Now.Ticks);

        private IoHeap<ConcurrentBag<string>> _dupHeap;
        private int _poolSize = 1000;
        private long _maxReq = int.MinValue;

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            var readOnlySequence =
                ReadOnlySequence.Slice(ReadOnlySequence.GetPosition(BufferOffset), ReadOnlySequence.GetPosition(BufferOffset + BytesRead));
            try
            {
                //fail fast
                if (BytesRead == 0 || Zeroed())
                    return State = IoJobMeta.JobState.ConInvalid;

                var read = 0;

                while(BytesLeftToProcess > 0 && State != IoJobMeta.JobState.ConInlined)
                {
                    CcWhisperMsg whispers = null;
                    //deserialize
                    try
                    {
                        readOnlySequence = readOnlySequence.Slice(readOnlySequence.GetPosition(read), readOnlySequence.GetPosition(BytesRead - read));
                        
                        whispers = CcWhisperMsg.Parser.ParseFrom(readOnlySequence);
                        if (whispers == null)
                            break;

                        read += whispers.CalculateSize();
                        State = IoJobMeta.JobState.Consumed;
                    }
                    catch (Exception e)
                    {
                        if (!Zeroed() && !MessageService.Zeroed())
                            _logger.Debug(e, $"Parse failed: r = {read}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={MemoryBuffer.Slice(BufferOffset - 2, 32).ToArray().HashSig()}, {Description}");

                        State = IoJobMeta.JobState.ConInlined;
                    }

                    if (read == 0)
                    {
                        continue;
                    }

                    Interlocked.Add(ref BufferOffset, (int)read);


                    //Sanity check the data
                    if (whispers == null || whispers.Data == null || whispers.Data.Length == 0)
                    {
                        continue;
                    }

                    var req = MemoryMarshal.Read<long>(whispers.Data.Span);
                    if (req > _maxReq)
                        _maxReq = req;
                    
                    try
                    {
                        if (!await CcCollective.DupSyncRoot.WaitAsync().ConfigureAwait(false))
                            return State = IoJobMeta.JobState.ConsumeErr;
                        
                        if (CcCollective.DupChecker.Count > _poolSize / 2)
                        {
                            var culled = CcCollective.DupChecker.Keys.Where(k => k < _maxReq - _poolSize / 4).ToList();
                            foreach (var mId in culled)
                            {
                                if (CcCollective.DupChecker.TryRemove(mId, out var del))
                                {
                                    del.Clear();
                                    _dupHeap.Return(del);
                                }
                            }    
                        }
                    }
                    finally
                    {
                        CcCollective.DupSyncRoot.Release();
                    }
                    
                        //set this message as seen if seen before
                    var endpoint = ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)(Source)).IoNetSocket.RemoteAddress;
                    _dupHeap.Take(out var dupEndpoints, endpoint);
                
                    if (dupEndpoints == null)
                        throw new OutOfMemoryException($"{_dupHeap}: {_dupHeap.ReferenceCount}/{_dupHeap.MaxSize} - c = {CcCollective.DupChecker.Count}, m = {_maxReq}");
                    
                    if (!CcCollective.DupChecker.TryAdd(req, dupEndpoints))
                    {
                        dupEndpoints.Clear();
                        _dupHeap.Return(dupEndpoints);
                        
                        //best effort
                        if (CcCollective.DupChecker.TryGetValue(req, out var endpoints))
                            endpoints.Add(endpoint);
                        
                        continue;
                    }

                    //if not seen before, set message as seen
                    var buf = whispers.ToByteArray();
                    async ValueTask ForwardMessage(IoNeighbor<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> drone)
                    {
                        try
                        {
                            var source = ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>) drone.Source);
                    
                            //Don't forward new messages to nodes from which we have received the msg in the mean time.
                            //This trick has the added bonus of using congestion as a governor to catch more of those overlaps, 
                            //which in turn lowers the traffic causing less congestion
                            if (source.IoNetSocket.RemoteAddress == endpoint ||
                                dupEndpoints.Contains(source.IoNetSocket.RemoteAddress))
                                return;
                            
                            var sentTask = source.IoNetSocket.SendAsync(Buffer, BufferOffset - BytesRead, BytesRead);
                            if (!sentTask.IsCompletedSuccessfully)
                                await sentTask.ConfigureAwait(false);
                    
                            if (sentTask.Result <= 0)
                            {
                                _logger.Trace($"Failed to forward new msg {req} message to {drone.Description}");
                            }
                            else
                            {
                                AutoPeeringEventService.AddEvent(new AutoPeerEvent
                                {
                                    EventType = AutoPeerEventType.SendProtoMsg,
                                    Msg = new ProtoMsg
                                    {
                                        CollectiveId = CcCollective.Hub.Router.Designation.IdString(),
                                        Id = ((CcDrone)drone).Adjunct.Designation.IdString(),
                                        Type = $"gossip{req % 6}"
                                    }
                                });
                            }
                        }
                        catch (Exception e)
                        {
                            _logger.Trace(e);
                        }
                    }
                    
                    async Task Forward(int broadCastDelay = 0)
                    {
                        if(broadCastDelay > 0)
                            await Task.Delay(broadCastDelay).ConfigureAwait(false);
                        
                        await CcCollective.WhisperingDrones.ForEachAsync(async d =>
                        {
                            await ForwardMessage(d).ConfigureAwait(false);
                        }).ConfigureAwait(false);    
                    }

                    var architect = _random.Next(16);
                    if (architect > 0)
                        await Forward(architect).ConfigureAwait(false);
                    else
                        Forward();
                }
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unmarshal Packet failed in {Description}");
            }
            finally
            {
                if (State == IoJobMeta.JobState.Consuming)
                    State = IoJobMeta.JobState.ConsumeErr;
                JobSync();
            }

            return State;
        }

        /// <summary>
        /// Processes a generic request
        /// </summary>
        /// <param name="packet">The packet</param>
        /// <typeparam name="T">The expected type</typeparam>
        /// <returns>The task</returns>
        private async ValueTask ProcessRequestAsync<T>(CcWhisperMsg packet)
            where T : IMessage<T>, IMessage, new()
        {
            //try
            //{
            //    var parser = new MessageParser<T>(() => new T());
            //    var request = parser.ParseFrom(packet.Data);

            //    if (request != null)
            //    {
            //        //_logger.Debug($"[{Base58Check.Base58CheckEncoding.Encode(packet.PublicKey.ToByteArray())}]{typeof(T).Name}: Received {packet.Data.Length}" );
            //        IIoZero zero = null;
            //        //if (((IoNetClient<CcPeerMessage>)Source).Socket.FfAddress != null)
            //        //    zero = IoZero;

            //        if (CurrBatch >= parm_max_msg_batch_size)
            //            await ForwardToNeighborAsync().ConfigureAwait(false);

            //        var remoteEp = new IPEndPoint(((IPEndPoint)ProducerExtraData).Address, ((IPEndPoint)ProducerExtraData).Port);
            //        ProtocolMsgBatch[CurrBatch] = ValueTuple.Create(zero, request, remoteEp, packet);
            //        Interlocked.Increment(ref CurrBatch);
            //    }
            //}
            //catch (NullReferenceException e)
            //{
            //    _logger.Trace(e, Description);
            //}
            //catch (Exception e)
            //{
            //    _logger.Error(e,
            //        $"Unable to parse request type {typeof(T).Name} from {Base58.Bitcoin.Encode(packet.PublicKey.Memory.AsArray())}, size = {packet.Data.Length}");
            //}
        }

        /// <summary>
        /// Forward jobs to conduit
        /// </summary>
        /// <returns>Task</returns>
        private async ValueTask ForwardToNeighborAsync()
        {
            try
            {
                if (CurrBatch == 0)
                    return;

                if (CurrBatch < parm_max_msg_batch_size)
                {
                    _protocolMsgBatch[CurrBatch] = default;
                }

                //cog the source
                var cogSuccess = await ProtocolConduit.Source.ProduceAsync(async (source, _, __, ioJob) =>
                {
                    var _this = (CcWhispers)ioJob;

                    if (!await ((CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>)source).EnqueueAsync(_this._protocolMsgBatch).ConfigureAwait(false))
                    {
                        if (!((CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>)source).Zeroed())
                            _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {_this.Description}");
                        return false;
                    }

                    //Retrieve batch buffer
                    try
                    {
                        _this._protocolMsgBatch = ArrayPool<CcGossipBatch>.Shared.Rent(_this.parm_max_msg_batch_size);
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e, $"Unable to rent from mempool: {_this.Description}");
                        return false;
                    }

                    _this.CurrBatch = 0;

                    return true;
                }, jobClosure: this).ConfigureAwait(false);

                ////forward transactions
                // if (cogSuccess)
                // {
                //     if (!await ProtocolConduit.ProduceAsync().ConfigureAwait(false))
                //     {
                //         _logger.Warn($"{TraceDescription} Failed to forward to `{ProtocolConduit.Source.Description}'");
                //     }
                // }
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Debug(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
            }
        }
    }
}
