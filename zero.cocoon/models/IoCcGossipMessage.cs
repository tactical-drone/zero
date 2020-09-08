using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.DataProtection.Repositories;
using NLog;
using Proto;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models.services;
using zero.core.conf;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;

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
        private async Task<int> SendMessage(ByteString data)
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

        public override async Task<JobState> ProduceAsync()
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
                        _producerStopwatch.Restart();
                        if (!Zeroed() && !await Source.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, AsyncTasks.Token).ConfigureAwait(false))
                        {
                            if (!Zeroed())
                            {
                                State = JobState.ProduceTo;
                                _producerStopwatch.Stop();
                                _logger.Debug($"{TraceDescription} timed out waiting for CONSUMER to release, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_producer_wait_for_consumer_timeout}ms', " +
                                              $"CB = `{Source.ConsumerBarrier.CurrentCount}'");

                                //TODO finish when config is fixed
                                //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                                //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) JobState.Consumed]) /
                                //         (Interlocked.Read(ref Source.Counters[(int) JobState.Consumed]) * 2 + 1));                                                                    
                            }
                            else
                                State = JobState.ProdCancel;
                            return false;
                        }

                        if (Zeroed())
                        {
                            State = JobState.ProdCancel;
                            return false;
                        }

                        //Async read the message from the message stream
                        if (Source.IsOperational)
                        {
                            await ((IoSocket)ioSocket).ReadAsync((byte[])(Array)Buffer, BufferOffset, BufferSize).AsTask().ContinueWith(rx =>
                            {
                                switch (rx.Status)
                                {
                                    //Canceled
                                    case TaskStatus.Canceled:
                                    case TaskStatus.Faulted:
                                        State = rx.Status == TaskStatus.Canceled ? JobState.ProdCancel : JobState.ProduceErr;
                                        Source.Zero(this);
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

                                        _logger.Trace($"{TraceDescription} RX=> read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLengthMax}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLengthMax) * 100)}%'");
                                        break;
                                    default:
                                        State = JobState.ProduceErr;
                                        throw new InvalidAsynchronousStateException($"Job =`{Description}', JobState={rx.Status}");
                                }
                            }, AsyncTasks.Token).ConfigureAwait(false);
                        }
                        else
                        {

                            Source.Zero(this);

                        }

                        if (Zeroed())
                        {
                            State = JobState.Cancelled;
                            return false;
                        }
                        return true;
                    }
                    catch (NullReferenceException){return false;}
                    catch (Exception e)
                    {
                        _logger.Debug(e,$"Error producing {Description}");
                        return false;
                    }
                }).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
            catch (NullReferenceException) { }
            catch (ObjectDisposedException) { }
            catch (OperationCanceledException) { }
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
            catch (ArgumentOutOfRangeException e ){ _logger.Debug(e, "Unmarshal Packet failed!"); }
            catch (NullReferenceException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
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
