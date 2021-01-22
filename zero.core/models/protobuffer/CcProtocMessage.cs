using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.core.conf;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.models.protobuffer
{
    public abstract class CcProtocMessage<TModel, TBatch> : IoMessage<CcProtocMessage<TModel, TBatch>>
    where TModel:IMessage
    {
        protected CcProtocMessage(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<TModel, TBatch>> source)
            : base(sinkDesc, jobDesc, source)
        {
            _logger = LogManager.GetCurrentClassLogger();

            //ProtocolMsgBatch = ArrayPool<ValueTuple<IIoZero, TModel, object, TModel>>.Shared.Rent(parm_max_msg_batch_size);

            DatumSize = 508;

            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize - 1;
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];
            ByteSegment = ByteBuffer;
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Message batch broadcast channel
        /// </summary>
        public IoConduit<CcProtocBatch<TModel, TBatch>> ProtocolConduit;

        /// <summary>
        /// Base source
        /// </summary>
        protected IoNetClient<CcProtocMessage<TModel, TBatch>> MessageService => (IoNetClient<CcProtocMessage<TModel, TBatch>>)Source;

        /// <summary>
        /// Used to control how long we wait for the source before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// The time a consumer will wait for a source to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting 

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 10;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ave_sec_ms = 2000;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ave_msg_sec_hist = 10 * 2;

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_msg_batch_size = 64;//TODO

        /// <summary>
        /// Message count 
        /// </summary>
        private int _msgCount = 0;

        /// <summary>
        /// Message rate
        /// </summary>
        private long _msgRateCheckpoint = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        /// <summary>
        /// Userdata in the source
        /// </summary>
        protected volatile object ProducerExtraData = new IPEndPoint(0, 0);

        /// <summary>
        /// How long to wait for the consumer before timing out
        /// </summary>
        public override int WaitForConsumerTimeout => parm_producer_wait_for_consumer_timeout;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if SAFE_RELEASE
            ProducerExtraData = null;
            ProtocolConduit = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        //public override async ValueTask ZeroManagedAsync()
        //{
        //    await base.ZeroManagedAsync().ConfigureAwait(false);
        //}

        private readonly IPEndPoint _remoteEp = new IPEndPoint(IPAddress.Any, 0);
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier,
            IIoZero zeroClosure)
        {
            try
            {
                await MessageService.ProduceAsync(async (ioSocket, producerPressure, ioZero, ioJob) =>
                {
                    var _this = (CcProtocMessage<TModel, TBatch>)ioJob;
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    if (!await producerPressure(ioJob, ioZero).ConfigureAwait(false))
                        return false;
                    try
                    {
                        //Async read the message from the message stream
                        if (_this.MessageService.IsOperational && !Zeroed())
                        {
                            int rx = await ((IoSocket)ioSocket).ReadAsync(_this.ByteSegment, _this.BufferOffset, _this.BufferSize, _this._remoteEp).ConfigureAwait(false);

                            //var readTask = ((IoSocket) ioSocket).ReadAsync(_this.ByteSegment, _this.BufferOffset,_this.BufferSize, _this._remoteEp, _this.MessageService.BlackList);
                            //await readTask.OverBoostAsync().ConfigureAwait(false);

                            //rx = readTask.Result;

                            //Success
                            //UDP signals source ip

                            ((IPEndPoint)_this.ProducerExtraData).Address = _this._remoteEp.Address;
                            ((IPEndPoint)_this.ProducerExtraData).Port = _this._remoteEp.Port;

                            //Drop zero reads
                            if (rx == 0)
                            {
                                _this.BytesRead = 0;
                                _this.State = IoJobMeta.JobState.ProduceTo;
                                return false;
                            }

                            //rate limit
                            _this._msgCount++;
                            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            var delta = now - _this._msgRateCheckpoint;
                            if (_this._msgCount > _this.parm_ave_sec_ms &&
                                (double)_this._msgCount * 1000 / delta > _this.parm_ave_sec_ms)
                            {
                                _this.BytesRead = 0;
                                _this.State = IoJobMeta.JobState.ProduceTo;
                                _this._logger.Fatal($"Dropping spam {_this._msgCount}");
                                _this._msgCount -= 2;
                                return false;
                            }

                            //hist reset
                            if (delta > _this.parm_ave_msg_sec_hist * 1000)
                            {
                                _this._msgRateCheckpoint = now;
                                _this._msgCount = 0;
                            }

                            _this.BytesRead = rx;

                            _this.UpdateBufferMetaData();

                            _this.State = IoJobMeta.JobState.Produced;

                            //_this._logger.Trace($"{_this.Description} => {GetType().Name}[{_this.Id}]: r = {_this.BytesRead}, r = {_this.BytesLeftToProcess}, dc = {_this.DatumCount}, ds = {_this.DatumSize}, f = {_this.DatumFragmentLength}, b = {_this.BytesLeftToProcess}/{_this.BufferSize + _this.DatumProvisionLengthMax}, b = {(int)(_this.BytesLeftToProcess / (double)(_this.BufferSize + _this.DatumProvisionLengthMax) * 100)}%");
                        }
                        else
                        {
                            _this._logger.Warn(
                                $"Source {_this.MessageService.Description} produce failed!");
                            _this.State = IoJobMeta.JobState.Cancelled;
                            await _this.MessageService.ZeroAsync(_this).ConfigureAwait(false);
                        }

                        if (_this.Zeroed())
                        {
                            _this.State = IoJobMeta.JobState.Cancelled;
                            return false;
                        }

                        return true;
                    }
                    catch (NullReferenceException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (TaskCanceledException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (OperationCanceledException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (ObjectDisposedException e)
                    {
                        _this._logger.Trace(e, _this.Description);
                        return false;
                    }
                    catch (Exception e)
                    {
                        _this.State = IoJobMeta.JobState.ProduceErr;
                        await _this.MessageService.ZeroAsync(_this).ConfigureAwait(false);
                        _this._logger.Error(e, $"ReadAsync {_this.Description}:");
                        return false;
                    }
                }, barrier, zeroClosure, this).ConfigureAwait(false);
                await Source.ProduceAsync((source, func, arg3, arg4) => new ValueTask<bool>());
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e, Description);
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"Producing job for {Description} returned with errors:");
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
        /// Handle fragments
        /// </summary>
        private void TransferPreviousBits()
        {
            if (!(PreviousJob?.StillHasUnprocessedFragments ?? false)) return;

            var p = (IoMessage<CcProtocMessage<TModel, TBatch>>)PreviousJob;
            try
            {
                var bytesToTransfer = Math.Min(p.DatumFragmentLength, DatumProvisionLengthMax);
                Interlocked.Add(ref BufferOffset, -bytesToTransfer);
                Interlocked.Add(ref BytesRead, bytesToTransfer);

                UpdateBufferMetaData();

                Array.Copy(p.Buffer, p.BufferOffset + Math.Max(p.BytesLeftToProcess - DatumProvisionLengthMax, 0),
                    Buffer, BufferOffset, bytesToTransfer);
            }
            catch (Exception e) // we de-synced 
            {
                _logger.Warn(e, $"{TraceDescription} We desynced!:");

                MessageService.Synced = false;
                DatumCount = 0;
                BytesRead = 0;
                State = IoJobMeta.JobState.ConInvalid;
                DatumFragmentLength = 0;
                StillHasUnprocessedFragments = false;
            }
        }

      

        /// <summary>
        /// offset into the batch
        /// </summary>
        protected volatile int CurrBatch;

    }
}
