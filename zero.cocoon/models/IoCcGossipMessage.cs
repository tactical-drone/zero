using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.core.conf;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using OperationCanceledException = System.OperationCanceledException;

namespace zero.cocoon.models
{
    /// <summary>
    /// Process gossip messages
    /// </summary>
    public class IoCcGossipMessage : IoMessage<IoCcGossipMessage>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobDescription">Describe the source</param>
        /// <param name="loadDescription">Describe the sink</param>
        /// <param name="originatingSource">The source of the work</param>
        /// <param name="zeroOnCascade">If false, don't allocate resources they will leak</param>
        public IoCcGossipMessage(string jobDescription, string loadDescription, IoSource<IoCcGossipMessage> originatingSource, bool zeroOnCascade = true) : base(loadDescription, jobDescription, originatingSource)
        {
            if (zeroOnCascade)
            {
                _logger = LogManager.GetCurrentClassLogger();

                DatumSize = parm_max_datum_size;

                //Init buffers
                BufferSize = DatumSize * parm_datums_per_buffer;
                DatumProvisionLengthMax = DatumSize - 1;
                //DatumProvisionLength = DatumProvisionLengthMax;
                Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];
            }
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The node that this message belongs to
        /// </summary>
        protected IoCcNode CcNode => ((IoCcPeer)IoZero).Neighbor.CcNode;

        /// <summary>
        /// Used to control how long we wait for the source before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// The time a consumer will wait for a source to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        //public int parm_producer_wait_for_consumer_timeout = 10*60 * 1000; //TODO make this adapting    
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting    

        /// <summary>
        /// The amount of items that can be ready for production before blocking
        /// </summary>
        [IoParameter]
        public int parm_forward_queue_length = 4;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 100;

        /// <summary>
        /// Max gossip message size
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_datum_size = 4;

        /// <summary>
        /// Userdata in the source
        /// </summary>
        protected volatile object ProducerUserData;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private async Task<int> SendMessageAsync(ByteString data)
        {
            var responsePacket = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(CcNode.CcId.PublicKey),
                Type = 0
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcNode.CcId.Sign(responsePacket.Data.ToByteArray(), 0, responsePacket.Data.Length));

            var msgRaw = responsePacket.ToByteArray();

            var sent = await ((IoTcpClient<IoCcGossipMessage>)Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length).ConfigureAwait(false);
            _logger.Debug($"{nameof(IoCcGossipMessage)}: Sent {sent} bytes to {((IoTcpClient<IoCcGossipMessage>)Source).Socket.RemoteAddress} ({Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), responsePacket.Type)})");
            return sent;
        }

        public override async Task<JobState> ProduceAsync(Func<IoJob<IoCcGossipMessage>, ValueTask<bool>> barrier)
        {
            try
            {
                if (Zeroed())
                    return State = JobState.ProdCancel;

                await Source.ProduceAsync(async ioSocket =>
                {
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    try
                    {
                        if (!await barrier(this))
                            return false;

                        //Async read the message from the message stream
                        if (Source.IsOperational)
                        {
                            await ((IoSocket)ioSocket).ReadAsync((byte[])(Array)Buffer, BufferOffset, BufferSize).AsTask().ContinueWith(async rx =>
                            {
                                switch (rx.Status)
                                {
                                    //Canceled
                                    case TaskStatus.Canceled:
                                    case TaskStatus.Faulted:
                                        State = rx.Status == TaskStatus.Canceled ? JobState.ProdCancel : JobState.ProduceErr;
                                        await Source.ZeroAsync(this).ConfigureAwait(false);
                                        _logger.Debug(rx.Exception?.InnerException, $"{TraceDescription} ReadAsync from stream returned with errors:");
                                        break;
                                    //Success
                                    case TaskStatus.RanToCompletion:
                                        BytesRead = rx.Result;

                                        //TODO WTF
                                        if (BytesRead == 0)
                                        {
                                            State = JobState.ProduceTo;
                                            break;
                                        }

                                        //UDP signals source ip
                                        ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                                        //Set how many datums we have available to process
                                        DatumCount = BytesLeftToProcess / DatumSize;
                                        DatumFragmentLength = BytesLeftToProcess % DatumSize;

                                        //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                                        StillHasUnprocessedFragments = DatumFragmentLength > 0;

                                        State = JobState.Produced;

                                        //_logger.Trace($"{TraceDescription} RX=> read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLengthMax}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLengthMax) * 100)}%'");
                                        break;
                                    default:
                                        State = JobState.ProduceErr;
                                        throw new InvalidAsynchronousStateException($"Job =`{Description}', JobState={rx.Status}");
                                }
                            }, AsyncTasks.Token).ConfigureAwait(false);
                        }
                        else
                        {

                            await Source.ZeroAsync(this).ConfigureAwait(false);

                        }

                        if (Zeroed())
                        {
                            State = JobState.Cancelled;
                            return false;
                        }
                        return true;
                    }
                    catch (NullReferenceException e){_logger.Trace(e, Description); return false;}
                    catch (TaskCanceledException e){ _logger.Trace(e, Description); return false; }
                    catch (ObjectDisposedException e) { _logger.Trace(e, Description); return false; }
                    catch (OperationCanceledException e) { _logger.Trace(e, Description); return false; }
                    catch (Exception e)
                    {
                        _logger.Debug(e,$"Error producing {Description}");
                        await Task.Delay(250, AsyncTasks.Token).ConfigureAwait(false); //TODO
                        return false;
                    }
                }).ConfigureAwait(false);
            }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Warn(e, $"{TraceDescription} Producing job returned with errors:");
            }
            finally
            {
                if (State == JobState.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    State = JobState.ProduceErr;
                }
            }
            return State;
        }

        private void TransferPreviousBits()
        {
            if (PreviousJob?.StillHasUnprocessedFragments ?? false)
            {
                var previousJobFragment = (IoMessage<IoCcGossipMessage>)PreviousJob;
                try
                {
                    var bytesToTransfer = previousJobFragment.DatumFragmentLength;
                    Interlocked.Add(ref BufferOffset, -bytesToTransfer);
                    //Interlocked.Add(ref DatumProvisionLength, -bytesToTransfer);
                    DatumCount = BytesLeftToProcess / DatumSize;
                    DatumFragmentLength = BytesLeftToProcess % DatumSize;
                    StillHasUnprocessedFragments = DatumFragmentLength > 0;

                    //TODO
                    Array.Copy(previousJobFragment.Buffer, previousJobFragment.BufferOffset, Buffer, BufferOffset, bytesToTransfer);
                }
                catch (Exception e) // we de-synced 
                {
                    _logger.Warn(e, $"{TraceDescription} We desynced!:");

                    Source.Synced = false;
                    DatumCount = 0;
                    BytesRead = 0;
                    State = JobState.Consumed;
                    DatumFragmentLength = 0;
                    StillHasUnprocessedFragments = false;
                }
            }

        }

        public override async Task<JobState> ConsumeAsync()
        {
            TransferPreviousBits();
            try
            {
                for (var i = 0; i < DatumCount; i++)
                {
                    var val = MemoryMarshal.Read<int>(BufferSpan.Slice(BufferOffset, 4));
                    if (val == ((IoCcPeer)IoZero).AccountingBit)
                    {
                        ((IoCcPeer)IoZero).AccountingBit = ++val;

                        MemoryMarshal.Write(BufferSpan.Slice(BufferOffset, 4), ref val);
                        await ((IoNetClient<IoCcGossipMessage>)Source).Socket.SendAsync(ByteBuffer, BufferOffset, 4).ConfigureAwait(false);
                        if ((val % 1000000) == 0)
                            _logger.Info($"4M>> {((IoCcPeer)IoZero).AccountingBit}");

                        Interlocked.Add(ref BufferOffset, DatumSize);
                        //if (Id % 10 == 0)
                        //    await Task.Delay(1, AsyncTasks.Token).ConfigureAwait(false);
                    }
                    else
                        _logger.Fatal($"{val} != {((IoCcPeer)IoZero).AccountingBit}");
                }
            }
            catch (ArgumentOutOfRangeException e ) { _logger.Trace(e, Description); }
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, "Unmarshal Packet failed!");
            }
            finally
            {
                UpdateBufferMetaData();
            }

            return State = JobState.Consumed;
        }
    }
}
