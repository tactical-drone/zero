using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Proto;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.cocoon.models.batches;
using zero.core.core;
using zero.core.misc;
using zero.core.models.protobuffer;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.cocoon.models
{
    public class CcWhispers : CcProtocMessage<CcWhisperMsg, CcGossipBatch>
    {
        public CcWhispers(string sinkDesc, string jobDesc, IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>> source, int concurrencyLevel = 1) : base(sinkDesc, jobDesc, source)
        {

            //_protocolMsgBatch = _arrayPool.Rent(parm_max_msg_batch_size);
            //_batchMsgHeap = new IoHeap<CcGossipBatch>(concurrencyLevel) { Make = o => new CcGossipBatch() };

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
            //if (_protocolMsgBatch != null)
            //    _arrayPool.ReturnAsync(_protocolMsgBatch, true);

            //_batchMsgHeap.ZeroManaged(batchMsg =>
            //{
            //    batchMsg.Zero = null;
            //    batchMsg.Message = null;
            //    batchMsg.RemoteEndPoint = null;
            //});

            _dupHeap.Clear();
            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            _dupHeap = null;
            //_arrayPool = null;
            //_batchMsgHeap = null;
            base.ZeroUnmanaged();
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
        private CcDrone CcDrone => ((CcDrone)IoZero);

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
        readonly Random _random = new Random((int)DateTime.Now.Ticks);

        private IoHeap<ConcurrentBag<string>> _dupHeap;
        private int _poolSize = 10000;
        private long _maxReq = int.MinValue;

        // private ArrayPool<CcGossipBatch> _arrayPool = ArrayPool<CcGossipBatch>.Shared;
        // private readonly int _concurrencyLevel;

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            var read = 0;
            try
            {
                //fail fast
                if (BytesRead == 0 || Zeroed())
                {
                    return State = IoJobMeta.JobState.ConInvalid;
                }

                var round = 0;
                while (BytesLeftToProcess > 0 && State != IoJobMeta.JobState.ConInlined)
                {
                    CcWhisperMsg whispers = null;
                    if(round++ > 1)
                        _logger.Fatal($"ROUND = {round}");
                        
                    //deserialize
                    try
                    {
                        whispers = CcWhisperMsg.Parser.ParseFrom(ReadOnlySequence.Slice(BufferOffset,
                            BytesLeftToProcess));
                        if (whispers == null)
                            break;

                        Interlocked.Add(ref BufferOffset, read = whispers.CalculateSize());
                        State = IoJobMeta.JobState.Consumed;
                    }
                    catch (Exception e)
                    {
                        if (!Zeroed() && !MessageService.Zeroed())
                            _logger.Debug(e,
                                $"Parse failed on round {round}: r = {read}/{BytesRead}/{BytesLeftToProcess}, d = {DatumCount}, b={MemoryBuffer.Slice(BufferOffset - 2, 32).ToArray().HashSig()}, {Description}");
                        
                        //try again
                        State = IoJobMeta.JobState.ConInlined;
                        Interlocked.Increment(ref BufferOffset);
                        continue;
                    }

                    if (read == 0)
                    {
                        continue;
                    }

                    //Sanity check the data
                    if (whispers == null || whispers.Data == null || whispers.Data.Length == 0)
                    {
                        continue;
                    }

                    //read the token
                    var req = MemoryMarshal.Read<long>(whispers.Data.Span);
                    if (req > _maxReq)
                        _maxReq = req;

                    try
                    {
                        if (!await CcCollective.DupSyncRoot.WaitAsync().FastPath().ConfigureAwait(false))
                            return State = IoJobMeta.JobState.ConsumeErr;

                        if (CcCollective.DupChecker.Count > _poolSize * 4 / 5)
                        {
                            var culled = CcCollective.DupChecker.Keys.Where(k => k < _maxReq - _poolSize / 2).ToList();
                            foreach (var mId in culled)
                            {
                                if (CcCollective.DupChecker.TryRemove(mId, out var del))
                                {
                                    del.Clear();
                                    await _dupHeap.ReturnAsync(del).FastPath().ConfigureAwait(false);
                                }
                            }
                        }
                    }
                    finally
                    {
                        try
                        {
                            await CcCollective.DupSyncRoot.ReleaseAsync().FastPath().ConfigureAwait(false);
                        }
                        catch (Exception) when (Zeroed())
                        {
                        }
                        catch (Exception e)
                        {
                            _logger.Fatal(e,$"{Description}, drone = {CcDrone}, adjunct = {CcAdjunct}, cc = {CcCollective}");
                            PrintStateHistory();
                        }
                        
                    }

                    if (State != IoJobMeta.JobState.Consumed)
                        continue;

                    //set this message as seen if seen before
                    var endpoint = ((IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)(Source)).IoNetSocket
                        .RemoteAddress;
                    _dupHeap.Take(out var dupEndpoints, endpoint);

                    if (dupEndpoints == null)
                        throw new OutOfMemoryException(
                            $"{_dupHeap}: {_dupHeap.ReferenceCount}/{_dupHeap.MaxSize} - c = {CcCollective.DupChecker.Count}, m = {_maxReq}");

                    if (!CcCollective.DupChecker.TryAdd(req, dupEndpoints))
                    {
                        dupEndpoints.Clear();
                        await _dupHeap.ReturnAsync(dupEndpoints).FastPath().ConfigureAwait(false);

                        //best effort
                        if (CcCollective.DupChecker.TryGetValue(req, out var endpoints))
                            endpoints.Add(endpoint);

                        continue;
                    }

                    var vt = ValueTuple.Create(this, read, endpoint, dupEndpoints);
                    await ZeroAsyncOptionAsync(static async state =>
                        {
                            var (@this, read, endpoint, dupEndpoints) = state;

                            //add small delays
                            int delay;
                            if ((delay = @this._random.Next(16)) > 0)
                                await Task.Delay(delay, @this.AsyncTasks.Token).ConfigureAwait(false);

                            var vt = ValueTuple.Create(@this, read, endpoint, dupEndpoints);
                            await @this.CcCollective.WhisperingDrones.ForEachAsync(static async (drone, state) =>
                            {
                                var (@this, read, endpoint, dupEndpoints) = state;
                                try
                                {
                                    var source =
                                        (IoNetClient<CcProtocMessage<CcWhisperMsg, CcGossipBatch>>)drone.Source;

                                    //Don't forward new messages to nodes from which we have received the msg in the mean time.
                                    //This trick has the added bonus of using congestion as a governor to catch more of those overlaps, 
                                    //which in turn lowers the traffic causing less congestion
                                    if (source.IoNetSocket.RemoteAddress == endpoint ||
                                        dupEndpoints.Contains(source.IoNetSocket.RemoteAddress))
                                        return;

                                    if (await source.IoNetSocket
                                        .SendAsync(@this.Buffer, @this.BufferOffset - read, read).FastPath()
                                        .ConfigureAwait(false) <= 0)
                                    {
                                        _logger.Trace($"Failed to forward new msg message to {drone.Description}");
                                    }
                                    else
                                    {
                                        AutoPeeringEventService.AddEvent(new AutoPeerEvent
                                        {
                                            EventType = AutoPeerEventType.SendProtoMsg,
                                            Msg = new ProtoMsg
                                            {
                                                CollectiveId = @this.CcCollective.Hub.Router.Designation.IdString(),
                                                Id = drone.Adjunct.Designation.IdString(),
                                                Type = $"gossip{@this.Id % 6}"
                                            }
                                        });
                                    }
                                }
                                catch (Exception e)
                                {
                                    _logger.Trace(e);
                                }

                            }, vt).FastPath().ConfigureAwait(false);

                        }, vt,
                        TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness |
                        TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
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
        private ValueTask ProcessRequestAsync<T>(CcWhisperMsg packet)
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

            //        if (CurrBatchSlot >= parm_max_msg_batch_size)
            //            await ForwardToNeighborAsync().ConfigureAwait(false);

            //        var remoteEp = new IPEndPoint(((IPEndPoint)RemoteEndPoint).Address, ((IPEndPoint)RemoteEndPoint).Port);
            //        ProtocolMsgBatch[CurrBatchSlot] = ValueTuple.Create(zero, request, remoteEp, packet);
            //        Interlocked.Increment(ref CurrBatchSlot);
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
            return ValueTask.CompletedTask;
        }

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

        //            if (!await ((CcProtocBatchSource<CcWhisperMsg, CcGossipBatch>)source).EnqueueAsync(_this._protocolMsgBatch).ConfigureAwait(false))
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
        //        }, jobClosure: this).ConfigureAwait(false);

        //        ////forward transactions
        //        // if (cogSuccess)
        //        // {
        //        //     if (!await ProtocolConduit.ProduceAsync().ConfigureAwait(false))
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
