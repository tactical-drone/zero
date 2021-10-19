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
using zero.core.patterns.misc;

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
        public uint parm_datums_per_buffer = 4;

        /// <summary>
        /// Max gossip message size
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_datum_size = 8;
        
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
                await sentTask.ConfigureAwait(Zc);

            //AutoPeeringEventService.AddEventAsync(new AutoPeerEvent
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
        /// <param name="nanite">Used to avoid variable captures</param>
        /// <returns>State</returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(Func<IIoJob, T, ValueTask<bool>> barrier, T nanite)
        {
            try
            {
                if (Zeroed())
                    return State = IoJobMeta.JobState.ProdCancel;

                var produced = await Source.ProduceAsync(static async (ioSocket, producerPressure, ioZero, ioJob) =>
                {
                    var @this = (CcPingPongTestMsg)ioJob;
                    
                    try
                    {
                        //----------------------------------------------------------------------------
                        // BARRIER
                        // We are only allowed to run ahead of the consumer by some configurable
                        // amount of steps. Instead of say just filling up memory buffers.
                        // This allows us some kind of (anti DOS?) congestion control
                        //----------------------------------------------------------------------------
                        if (!await producerPressure(ioJob, ioZero).ConfigureAwait(@this.Zc))
                            return false;

                        //Async read the message from the message stream
                        if (@this.Source.IsOperational)
                        {
                            @this.BytesRead += (uint)await ((IoSocket)ioSocket)
                                .ReadAsync(@this.ArraySegment, (int)@this.BufferOffset, (int)@this.BufferSize).FastPath()
                                .ConfigureAwait(@this.Zc);

                            //TODO WTF
                            if (@this.BytesRead == 0)
                            {
                                @this.State = IoJobMeta.JobState.ProduceTo;
                                return false;
                            }

                            //Set how many datums we have available to process
                            @this.DatumCount = @this.BytesLeftToProcess / @this.DatumSize;
                            @this.DatumFragmentLength = @this.BytesLeftToProcess % @this.DatumSize;

                            //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                            @this.Syncing = @this.DatumFragmentLength > 0;

                            @this.State = IoJobMeta.JobState.Produced;

                            //_logger.Trace($"{TraceDescription} RX=> read=`{BytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLengthMax}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLengthMax) * 100)}%'");
                            
                        }
                        else
                        {
                            await @this.Source.ZeroAsync(@this).FastPath().ConfigureAwait(@this.Zc);
                        }

                        if (@this.Zeroed())
                        {
                            @this.State = IoJobMeta.JobState.Cancelled;
                            return false;
                        }
                        return true;
                    }
                    catch (NullReferenceException e){ _logger.Trace(e, @this.Description); return false;}
                    catch (TaskCanceledException e){ _logger.Trace(e, @this.Description); return false; }
                    catch (ObjectDisposedException e) { _logger.Trace(e, @this.Description); return false; }
                    catch (OperationCanceledException e) { _logger.Trace(e, @this.Description); return false; }
                    catch (Exception e)
                    {
                        if(!@this.Zeroed() && !(e is SocketException))
                            _logger.Debug(e,$"Error producing {@this.Description}");
                        
                        await Task.Delay(100).ConfigureAwait(@this.Zc); //TODO

                        @this.State = IoJobMeta.JobState.ProduceErr;

                        if(@this.Source != null)
                            await @this.Source.ZeroAsync(@this).ConfigureAwait(@this.Zc);

                        return false;
                    }
                }, this, barrier, nanite).ConfigureAwait(Zc);

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
            if (PreviousJob?.Syncing ?? false)
            {
                var previousJobFragment = (IoMessage<CcPingPongTestMsg>)PreviousJob;
                try
                {
                    var bytesToTransfer = previousJobFragment.DatumFragmentLength;
                    BufferOffset -= bytesToTransfer;
                    //Interlocked.Add(ref DatumProvisionLength, -bytesToTransfer);
                    DatumCount = BytesLeftToProcess / DatumSize;
                    DatumFragmentLength = BytesLeftToProcess % DatumSize;
                    Syncing = DatumFragmentLength > 0;

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
                    Syncing = false;
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
                    var req = MemoryMarshal.Read<long>(MemoryBuffer.Span.Slice((int)BufferOffset, (int)DatumSize));
                    var exp = Interlocked.Read(ref ((CcDrone) IoZero).AccountingBit);
                    if (req == exp)
                    {
                        
                        req++;
                        MemoryMarshal.Write(MemoryBuffer.Span.Slice((int)BufferOffset, (int)DatumSize), ref req);

                        //if (Id % 10 == 0)
                        await Task.Delay(250, AsyncTasks.Token).ConfigureAwait(Zc);

                        var sentTask = ((IoNetClient<CcPingPongTestMsg>) Source).IoNetSocket.SendAsync(ArraySegment, (int)BufferOffset, (int)DatumSize);

                        if (!sentTask.IsCompletedSuccessfully)
                            await sentTask.ConfigureAwait(Zc);

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

                            MemoryMarshal.Write(MemoryBuffer.Span.Slice((int)BufferOffset, (int)DatumSize), ref req);
                            if (await ((IoNetClient<CcPingPongTestMsg>) Source).IoNetSocket
                                .SendAsync(ArraySegment, (int)BufferOffset, (int)DatumSize).ConfigureAwait(Zc) > 0)
                            {
                                Volatile.Write(ref ((CcDrone)IoZero).AccountingBit, 2);
                            }
                        }
                        else
                        {
                            _logger.Fatal($"({DatumCount}) SET! {req} != {exp}");

                            req = 0;
                            MemoryMarshal.Write(MemoryBuffer.Span.Slice((int)BufferOffset, (int)DatumSize), ref req);
                            if (await ((IoNetClient<CcPingPongTestMsg>) Source).IoNetSocket
                                .SendAsync(ArraySegment, (int)BufferOffset, (int)DatumSize).ConfigureAwait(Zc) > 0)
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
