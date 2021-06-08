using System;
using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using SimpleBase;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.core;
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.models.protobuffer.sources;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.cocoon.models
{
    public class CcWispers : CcProtocMessage<CcWisperMsg, CcGossipBatch>
    {
        public CcWispers(string sinkDesc, string jobDesc, IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>> source) : base(sinkDesc, jobDesc, source)
        {
            
        }

        //public override async ValueTask<bool> ConstructAsync()
        //{
        //    if (!MessageService.ObjectStorage.ContainsKey($"{nameof(CcProtocMessage<CcWisperMsg, CcGossipBatch>)}.Gossip"))
        //    {
        //        CcProtocBatchSource<CcWisperMsg, CcGossipBatch> channelSource = null;

        //        //Transfer ownership
        //        if (await MessageService.ZeroAtomicAsync((s, u, d) =>
        //        {
        //            channelSource = new CcProtocBatchSource<CcWisperMsg, CcGossipBatch>(MessageService, _arrayPool, 0, Source.ConcurrencyLevel * 2);
        //            if (MessageService.ObjectStorage.TryAdd($"{nameof(CcProtocMessage<CcWisperMsg, CcGossipBatch>)}.Gossip", channelSource))
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
        //                userData => new CcProtocBatch<CcWisperMsg, CcGossipBatch>(channelSource, -1 /*We block to control congestion*/),
        //                Source.ConcurrencyLevel * 2, Source.ConcurrencyLevel * 2
        //            ).ConfigureAwait(false);

        //            //get reference to a central mem pool
        //            if (ProtocolConduit != null)
        //                _arrayPool = ((CcProtocBatchSource<CcWisperMsg, CcGossipBatch>)ProtocolConduit.Source).ArrayPool;
        //            else
        //                return false;
        //        }
        //        else
        //        {
        //            var t = channelSource.ZeroAsync(new IoNanoprobe("Lost race on creation"));
        //            ProtocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<CcWisperMsg, CcGossipBatch>>(nameof(CcDrone)).ConfigureAwait(false);
        //        }
        //    }
        //    else
        //    {
        //        ProtocolConduit = await MessageService.AttachConduitAsync<CcProtocBatch<CcWisperMsg, CcGossipBatch>>(nameof(CcDrone)).ConfigureAwait(false);
        //        return ProtocolConduit != null;
        //    }

        //    return await base.ConstructAsync().ConfigureAwait(false);
        //}
        
        /// <summary>
        /// Batch of messages
        /// </summary>
        private volatile CcGossipBatch[] ProtocolMsgBatch;

        /// <summary>
        /// message heap
        /// </summary>
        private ArrayPool<CcGossipBatch> _arrayPool = ArrayPool<CcGossipBatch>.Create();

        public ArrayPool<CcGossipBatch> ArrayPool => _arrayPool;

        /// <summary>
        /// CC Node
        /// </summary>
        protected CcCollective CcCollective => ((CcDrone)IoZero)?.Adjunct?.CcCollective;

        ///// <summary>
        ///// Cc Identity
        ///// </summary>
        public CcDesignation CcId => CcCollective.CcId;

        /// <summary>
        /// random number generator
        /// </summary>
        readonly Random _random = new Random((int)DateTime.Now.Ticks);

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
                
                for (var i = 0; i <= DatumCount && BytesLeftToProcess > 0; i++)
                {
                    if (DatumCount > 1)
                    {
                        _logger.Fatal($"{Description} ----> Datumcount = {DatumCount}");
                    }

                    readOnlySequence = readOnlySequence.Slice(readOnlySequence.GetPosition(read), readOnlySequence.GetPosition(BytesRead - read));
                    CcWisperMsg wispers = null;

                    //deserialize
                    try
                    {
                        wispers = CcWisperMsg.Parser.ParseFrom(readOnlySequence);
                        if (wispers == null)
                            break;

                        read += wispers.CalculateSize();
                    }
                    catch (Exception e)
                    {
                        var tmpBufferOffset = BufferOffset;

                        if (!Zeroed() && !MessageService.Zeroed())
                            _logger.Debug(e, $"Parse failed: r = {read}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={MemoryBuffer.Slice(tmpBufferOffset - 2, 32).ToArray().HashSig()}, {Description}");

                        if (read > 0)
                        {
                            continue;
                        }
                        else
                            break;
                    }

                    Interlocked.Add(ref BufferOffset, (int)read);

#if !DEBUG
                    if (read == 0)
                    {
                        continue;
                    }
#endif

                    if (DatumCount > 0)
                    {
                        if (i == 0 && BytesLeftToProcess > 0)
                        {
                            _logger.Debug($"MULTI<<D = {DatumCount}, r = {BytesRead}, d = {read}, l = {BytesLeftToProcess}>>");
                        }
                        if (i == 1) //&& read > 0)
                        {
                            _logger.Debug($"MULTI - READ <<D = {DatumCount}, r = {BytesRead}, d = {read}, l = {BytesLeftToProcess}>>");
                        }
                    }

                    //Sanity check the data
                    if (wispers.Data == null || wispers.Data.Length == 0)
                    {
                        continue;
                    }

                    var req = MemoryMarshal.Read<long>(wispers.Data.Span);
                    var endpoint = ((IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>>)(Source)).IoNetSocket.RemoteAddress;
                    ConcurrentBag<string> dupEndpoints = null;

                    if (CcCollective.DupChecker.Count > 100)
                    {
                        CcCollective.DupChecker.Clear();
                    }

                    //set this message as seen if seen before
                    Func<bool> dupcheck = () =>
                    {
                        if (CcCollective.DupChecker.TryGetValue(req, out dupEndpoints))
                        {
                            dupEndpoints.Add(endpoint);
                            return true;
                        }

                        return false;
                    };

                    //if not seen before, set message as seen
                    if (!dupcheck() && CcCollective.DupChecker.TryAdd(req, dupEndpoints = new ConcurrentBag<string>(new[] { endpoint })))
                    {
                        var buf = wispers.ToByteArray();

                        async void ForwardMessage(IoNeighbor<CcProtocMessage<CcWisperMsg, CcGossipBatch>> drone)
                        {
                            try
                            {
                                var source = ((IoNetClient<CcProtocMessage<CcWisperMsg, CcGossipBatch>>) drone.Source);

                                //Don't forward new messages to nodes from which we have received the msg in the mean time.
                                //This trick has the added bonus of using congestion as a governor to catch more of those overlaps, 
                                //which in turn lowers the traffic causing less congestion
                                if (source.IoNetSocket.RemoteAddress == endpoint || dupEndpoints.Contains(source.IoNetSocket.RemoteAddress))
                                    return;

                                var sentTask = source.IoNetSocket.SendAsync(buf, 0, buf.Length);
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
                                            Type = $"gossip{req % 4}"
                                        }
                                    });
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Trace(e);
                            }
                        }

                        await Task.Delay(_random.Next(100) + 10).ConfigureAwait(false);

                        await CcCollective.WisperingDrones.ForEachAsync(d =>
                        {
                            ForwardMessage(d);
                            return ValueTask.CompletedTask;
                        });
                    }
                    else
                    {
                        if (!dupcheck())
                        {
                            _logger.Fatal($"Unexpected: source drone {endpoint} not found");
                        }
                    }
                }

                //Release a waiter
                //await ForwardToNeighborAsync().ConfigureAwait(false);

                State = IoJobMeta.JobState.Consumed;
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
        private async ValueTask ProcessRequestAsync<T>(CcWisperMsg packet)
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
                    ProtocolMsgBatch[CurrBatch] = default;
                }

                //cog the source
                var cogSuccess = await ProtocolConduit.Source.ProduceAsync(async (source, _, __, ioJob) =>
                {
                    var _this = (CcWispers)ioJob;

                    if (!await ((CcProtocBatchSource<CcWisperMsg, CcGossipBatch>)source).EnqueueAsync(_this.ProtocolMsgBatch).ConfigureAwait(false))
                    {
                        if (!((CcProtocBatchSource<CcWisperMsg, CcGossipBatch>)source).Zeroed())
                            _logger.Fatal($"{nameof(ForwardToNeighborAsync)}: Unable to q batch, {_this.Description}");
                        return false;
                    }

                    //Retrieve batch buffer
                    try
                    {
                        _this.ProtocolMsgBatch = ArrayPool<CcGossipBatch>.Shared.Rent(_this.parm_max_msg_batch_size);
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
