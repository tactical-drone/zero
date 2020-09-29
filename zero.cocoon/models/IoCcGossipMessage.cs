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
using zero.core.misc;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
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
                ByteSegment = ByteBuffer;
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
        public int parm_max_datum_size = 8;

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
                ByteString.CopyFrom(CcNode.CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length));

            var msgRaw = responsePacket.ToByteArray();
            
            var sentTask =  ((IoTcpClient<IoCcGossipMessage>)Source).Socket.SendAsync(msgRaw, 0, msgRaw.Length);

            if (!sentTask.IsCompletedSuccessfully)
                await sentTask.ConfigureAwait(false);
            
            _logger.Debug($"{nameof(IoCcGossipMessage)}: Sent {sentTask.Result} bytes to {((IoTcpClient<IoCcGossipMessage>)Source).Socket.RemoteAddress} ({Enum.GetName(typeof(IoCcPeerMessage.MessageTypes), responsePacket.Type)})");
            return sentTask.Result;
        }

        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {
            try
            {
                if (Zeroed())
                    return State = IoJobMeta.JobState.ProdCancel;

                var produced = await Source.ProduceAsync(async (ioSocket, producerPressure, ioZero, ioJob) =>
                {
                    var _this = (IoCcGossipMessage)ioJob;
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    try
                    {
                        if (!await producerPressure(ioJob, ioZero).ConfigureAwait(false))
                            return false;

                        //Async read the message from the message stream
                        if (_this.Source.IsOperational)
                        {
                            var readTask = ((IoSocket)ioSocket).ReadAsync(_this.ByteSegment, _this.BufferOffset, _this.BufferSize);

                            //slow path
                            if (!readTask.IsCompletedSuccessfully)
                                _this.BytesRead = await readTask.ConfigureAwait(false);
                            else
                                _this.BytesRead = readTask.Result;

                            //TODO WTF
                            if (_this.BytesRead == 0)
                            {
                                _this.State = IoJobMeta.JobState.ProduceTo;
                                return false;
                            }

                            //UDP signals source ip
                            _this.ProducerUserData = ((IoSocket)ioSocket).ExtraData();

                            //Set how many datums we have available to process
                            _this.DatumCount = _this.BytesLeftToProcess / _this.DatumSize;
                            _this.DatumFragmentLength = _this.BytesLeftToProcess % _this.DatumSize;

                            //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                            _this.StillHasUnprocessedFragments = _this.DatumFragmentLength > 0;

                            _this.State = IoJobMeta.JobState.Produced;

                            //_logger.Trace($"{TraceDescription} RX=> read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLengthMax}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLengthMax) * 100)}%'");
                            
                        }
                        else
                        {
                            await _this.Source.ZeroAsync(_this).ConfigureAwait(false);
                        }

                        if (_this.Zeroed())
                        {
                            _this.State = IoJobMeta.JobState.Cancelled;
                            return false;
                        }
                        return true;
                    }
                    catch (NullReferenceException e){ _this._logger.Trace(e, Description); return false;}
                    catch (TaskCanceledException e){ _this._logger.Trace(e, Description); return false; }
                    catch (ObjectDisposedException e) { _this._logger.Trace(e, Description); return false; }
                    catch (OperationCanceledException e) { _this._logger.Trace(e, Description); return false; }
                    catch (Exception e)
                    {
                        _logger.Debug(e,$"Error producing {_this.Description}");
                        await Task.Delay(250, _this.AsyncTokenProxy.Token).ConfigureAwait(false); //TODO

                        _this.State = IoJobMeta.JobState.ProduceErr;

                        await _this.Source.ZeroAsync(_this).ConfigureAwait(false);

                        return false;
                    }
                }, barrier, zeroClosure, this).ConfigureAwait(false);

                if (!produced)
                {
                    State = IoJobMeta.JobState.ProduceTo;
                }
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
                if (State == IoJobMeta.JobState.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    State = IoJobMeta.JobState.ProduceErr;
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
                    State = IoJobMeta.JobState.Consumed;
                    DatumFragmentLength = 0;
                    StillHasUnprocessedFragments = false;
                }
            }

        }

        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            TransferPreviousBits();
            try
            {
                for (var i = 0; i < DatumCount; i++)
                {
                    var req = MemoryMarshal.Read<long>(BufferSpan.Slice(BufferOffset, DatumSize));
                    var exp = Interlocked.Read(ref ((IoCcPeer) IoZero).AccountingBit);
                    if (req == exp)
                    {
                        //_logger.Warn($"MATCH {((IoCcPeer)IoZero).AccountingBit}");

                        req++;
                        MemoryMarshal.Write(BufferSpan.Slice(BufferOffset, DatumSize), ref req);

                        //if (Id % 10 == 0)
                        //await Task.Delay(3, AsyncTokenProxy.Token).ConfigureAwait(false);

                        var sentTask = ((IoNetClient<IoCcGossipMessage>) Source).Socket.SendAsync(ByteSegment, BufferOffset, DatumSize);
                        
                        //slow path
                        if (!sentTask.IsCompletedSuccessfully)
                            await sentTask.ConfigureAwait(false);
                        
                        if (sentTask.Result > 0)
                        {
                            Interlocked.Add(ref ((IoCcPeer) IoZero).AccountingBit, 2);
                        }

                        //if ((req % 1000) == 0)
                        //    _logger.Info($"1000>> {exp}");
                    }
                    else
                    {
                        //reset
                        if (req == 0)
                        {
                            _logger.Fatal($"({DatumCount}) RESET! {req} != {exp}");

                            req = 1;

                            MemoryMarshal.Write(BufferSpan.Slice(BufferOffset, DatumSize), ref req);
                            if (await ((IoNetClient<IoCcGossipMessage>) Source).Socket
                                .SendAsync(ByteSegment, BufferOffset, DatumSize).ConfigureAwait(false) > 0)
                            {
                                Volatile.Write(ref ((IoCcPeer)IoZero).AccountingBit, 2);
                            }
                        }
                        else
                        {
                            _logger.Fatal($"({DatumCount}) SET! {req} != {exp}");

                            req = 0;
                            MemoryMarshal.Write(BufferSpan.Slice(BufferOffset, DatumSize), ref req);
                            if (await ((IoNetClient<IoCcGossipMessage>) Source).Socket
                                .SendAsync(ByteSegment, BufferOffset, DatumSize).ConfigureAwait(false) > 0)
                            {
                                Volatile.Write(ref ((IoCcPeer)IoZero).AccountingBit, 1);
                            }
                        }
                    }

                    Interlocked.Add(ref BufferOffset, DatumSize);
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

            return State = IoJobMeta.JobState.Consumed;
        }
    }
}
