using System;
using System.Buffers;
using System.IO;
using System.Net.Sockets;
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

namespace zero.cocoon.models.test
{
    /// <summary>
    /// Process gossip messages
    /// </summary>
    public class CcPingPongTestMsg : IoMessage<CcPingPongTestMsg>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobDesc">Describe the source</param>
        /// <param name="sinkDesc">Describe the sink</param>
        /// <param name="originatingSource">The source of the work</param>
        /// <param name="zeroOnCascade">If false, don't allocate resources they will leak</param>
        public CcPingPongTestMsg(string jobDesc, string sinkDesc, IoSource<CcPingPongTestMsg> originatingSource, bool zeroOnCascade = true) : base(sinkDesc, jobDesc, originatingSource)
        {
            if (zeroOnCascade)
            {

                DatumSize = parm_max_datum_size;

                //Init buffers
                BufferSize = DatumSize * parm_datums_per_buffer;
                DatumProvisionLengthMax = DatumSize - 1;
                //DatumProvisionLength = DatumProvisionLengthMax;
                Buffer = new byte[BufferSize + DatumProvisionLengthMax];
                ArraySegment = new ArraySegment<byte>(Buffer);
                ReadOnlySequence = new ReadOnlySequence<byte>(Buffer);
                ByteStream = new MemoryStream(Buffer);
            }
        }
        
        /// <summary>
        /// The node that this message belongs to
        /// </summary>
        protected CcCollective CcCollective => ((CcDrone)IoZero).Adjunct.CcCollective;
        
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
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private async Task<int> SendMessageAsync(ByteString data)
        {
            var responsePacket = new Packet
            {
                Data = data,
                PublicKey = ByteString.CopyFrom(CcCollective.CcId.PublicKey),
                Type = 0
            };

            responsePacket.Signature =
                ByteString.CopyFrom(CcCollective.CcId.Sign(responsePacket.Data.Memory.AsArray(), 0, responsePacket.Data.Length));

            var msgRaw = responsePacket.ToByteArray();
            
            var sentTask =  ((IoTcpClient<CcPingPongTestMsg>)Source).IoNetSocket.SendAsync(msgRaw, 0, msgRaw.Length);

            if (!sentTask.IsCompletedSuccessfully)
                await sentTask.ConfigureAwait(false);

            //AutoPeeringEventService.AddEvent(new AutoPeerEvent
            //{
            //    EventType = AutoPeerEventType.SendProtoMsg,
            //    Msg = new ProtoMsg
            //    {
            //        CollectiveId =  Hub.Router.Designation.IdString(),
            //        Id = Designation.IdString(),
            //        Type = "ping"
            //    }
            //});


            //_logger.Debug($"{nameof(CcPingPongTestMsg)}: Sent {sentTask.Result} bytes to {((IoTcpClient<CcPingPongTestMsg>)Source).IoNetSocket.RemoteAddress} ({Enum.GetName(typeof(CcSubspaceMessage.MessageTypes), responsePacket.Type)})");

            return sentTask.Result;
        }

        /// <summary>
        /// produce a gossip message
        /// </summary>
        /// <param name="barrier">Used to synchronize</param>
        /// <param name="zeroClosure">Used to avoid variable captures</param>
        /// <returns>State</returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {
            try
            {
                if (Zeroed())
                    return State = IoJobMeta.JobState.ProdCancel;

                var produced = await Source.ProduceAsync(async (ioSocket, producerPressure, ioZero, ioJob) =>
                {
                    var _this = (CcPingPongTestMsg)ioJob;
                    
                    try
                    {
                        //----------------------------------------------------------------------------
                        // BARRIER
                        // We are only allowed to run ahead of the consumer by some configurable
                        // amount of steps. Instead of say just filling up memory buffers.
                        // This allows us some kind of (anti DOS?) congestion control
                        //----------------------------------------------------------------------------
                        if (!await producerPressure(ioJob, ioZero).ConfigureAwait(false))
                            return false;

                        //Async read the message from the message stream
                        if (_this.Source.IsOperational)
                        {
                            var readTask = ((IoSocket)ioSocket).ReadAsync(_this.ArraySegment, _this.BufferOffset, _this.BufferSize);

                            //slow path
                            if (!readTask.IsCompletedSuccessfully)
                                Interlocked.Add(ref _this.BytesRead, await readTask.ConfigureAwait(false));
                            else
                                Interlocked.Add(ref _this.BytesRead, readTask.Result);

                            //TODO WTF
                            if (_this.BytesRead == 0)
                            {
                                _this.State = IoJobMeta.JobState.ProduceTo;
                                return false;
                            }

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
                    catch (NullReferenceException e){ _logger.Trace(e, _this.Description); return false;}
                    catch (TaskCanceledException e){ _logger.Trace(e, _this.Description); return false; }
                    catch (ObjectDisposedException e) { _logger.Trace(e, _this.Description); return false; }
                    catch (OperationCanceledException e) { _logger.Trace(e, _this.Description); return false; }
                    catch (Exception e)
                    {
                        if(!_this.Zeroed() && !(e is SocketException))
                            _logger.Debug(e,$"Error producing {_this.Description}");
                        
                        await Task.Delay(100).ConfigureAwait(false); //TODO

                        _this.State = IoJobMeta.JobState.ProduceErr;

                        if(_this.Source != null)
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
        
        /// <summary>
        /// Transfers previous job bits to this one
        /// </summary>
        private void TransferPreviousBits()
        {
            if (PreviousJob?.StillHasUnprocessedFragments ?? false)
            {
                var previousJobFragment = (IoMessage<CcPingPongTestMsg>)PreviousJob;
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

        /// <summary>
        /// Process a gossip message
        /// </summary>
        /// <returns>The state</returns>
        public override async ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            TransferPreviousBits();
            try
            {
                for (var i = 0; i < DatumCount; i++)
                {
                    var req = MemoryMarshal.Read<long>(MemoryBuffer.Span.Slice(BufferOffset, DatumSize));
                    var exp = Interlocked.Read(ref ((CcDrone) IoZero).AccountingBit);
                    if (req == exp)
                    {
                        
                        req++;
                        MemoryMarshal.Write(MemoryBuffer.Span.Slice(BufferOffset, DatumSize), ref req);

                        //if (Id % 10 == 0)
                        await Task.Delay(250, AsyncTasks.Token).ConfigureAwait(false);

                        var sentTask = ((IoNetClient<CcPingPongTestMsg>) Source).IoNetSocket.SendAsync(ArraySegment, BufferOffset, DatumSize);

                        if (!sentTask.IsCompletedSuccessfully)
                            await sentTask.ConfigureAwait(false);

                        if (sentTask.Result > 0)
                        {
                            Interlocked.Add(ref ((CcDrone) IoZero).AccountingBit, 2);
                        }
                    }
                    else
                    {
                        //reset
                        if (req == 0)
                        {
                            _logger.Fatal($"({DatumCount}) RESET! {req} != {exp}");

                            req = 1;

                            MemoryMarshal.Write(MemoryBuffer.Span.Slice(BufferOffset, DatumSize), ref req);
                            if (await ((IoNetClient<CcPingPongTestMsg>) Source).IoNetSocket
                                .SendAsync(ArraySegment, BufferOffset, DatumSize).ConfigureAwait(false) > 0)
                            {
                                Volatile.Write(ref ((CcDrone)IoZero).AccountingBit, 2);
                            }
                        }
                        else
                        {
                            _logger.Fatal($"({DatumCount}) SET! {req} != {exp}");

                            req = 0;
                            MemoryMarshal.Write(MemoryBuffer.Span.Slice(BufferOffset, DatumSize), ref req);
                            if (await ((IoNetClient<CcPingPongTestMsg>) Source).IoNetSocket
                                .SendAsync(ArraySegment, BufferOffset, DatumSize).ConfigureAwait(false) > 0)
                            {
                                Volatile.Write(ref ((CcDrone)IoZero).AccountingBit, 1);
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
                JobSync();
            }

            return State = IoJobMeta.JobState.Consumed;
        }
    }
}
