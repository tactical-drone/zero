﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.interop.entangled.common.model;
using zero.interop.entangled.interfaces;
using zero.interop.entangled.mock;
using zero.tangle.api.controllers.generic;
using zero.tangle.entangled;
using zero.tangle.models.sources;
using Logger = NLog.Logger;

namespace zero.tangle.models
{
    /// <summary>
    /// Specializes a generic <see cref="IoMessage{TProducer}"/> into a specific one for the tangle. This class contains details of how a message is to be 
    /// extracted from <see cref="IoMessage{TProducer}"/>
    /// </summary>
    public sealed class IoTangleMessage<TKey> : IoMessage<IoTangleMessage<TKey>>        
    {
        /// <summary>
        /// Constructs buffers that hold tangle message information
        /// </summary>
        /// <param name="jobDescription">A description of the job needed to do the work</param>
        /// <param name="jobDescriptionn">A description of the work that needs to be done</param>
        /// <param name="source">The upstream source where messages are coming from</param>
        public IoTangleMessage(string jobDescription, string loadDescription, IoSource<IoTangleMessage<TKey>> source):base(loadDescription, loadDescription, source)
        {
            _logger = LogManager.GetCurrentClassLogger();

            _entangled = Entangled<TKey>.Default;            

            //Set some tangle specific protocol constants
            DatumSize = Codec.MessageSize + ((Source is IoTcpClient<IoTangleMessage<TKey>>) ? Codec.MessageCrcSize : 0);
            
            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize - 1;
            DatumProvisionLength = DatumProvisionLengthMax;
            Buffer = new sbyte[BufferSize + DatumProvisionLengthMax];

            //forward to node services
            if (!Source.ObjectStorage.ContainsKey(nameof(_nodeServicesProxy)))
            {
                _nodeServicesProxy = new IoTangleTransactionSource<TKey>($"{nameof(_nodeServicesProxy)}", parm_forward_queue_length);
                if (!Source.ObjectStorage.TryAdd(nameof(_nodeServicesProxy), _nodeServicesProxy))
                {
                    _nodeServicesProxy = (IoTangleTransactionSource<TKey>)Source.ObjectStorage[nameof(_nodeServicesProxy)];
                }
            }

            NodeServicesArbiter = Source.AttachProducer(nameof(IoNodeServices<TKey>), false, _nodeServicesProxy, userData => new IoTangleTransaction<TKey>(_nodeServicesProxy));            


            NodeServicesArbiter.parm_consumer_wait_for_producer_timeout = 0; 
            NodeServicesArbiter.parm_producer_start_retry_time = 0;

            //forward to neighbor
            if (!Source.ObjectStorage.ContainsKey(nameof(_neighborProducer)))
            {
                _neighborProducer = new IoTangleTransactionSource<TKey>($"{nameof(_neighborProducer)}", parm_forward_queue_length);
                if (!Source.ObjectStorage.TryAdd(nameof(_neighborProducer), _neighborProducer))
                {
                    _neighborProducer = (IoTangleTransactionSource<TKey>)Source.ObjectStorage[nameof(_neighborProducer)];
                }
            }

            NeighborServicesArbiter = Source.AttachProducer(nameof(TanglePeer<IoTangleTransaction<TKey>>), false, _neighborProducer, userData => new IoTangleTransaction<TKey>(_neighborProducer, -1 /*We block to control congestion*/));                        
            NeighborServicesArbiter.parm_consumer_wait_for_producer_timeout = -1; //We block and never report slow production
            NeighborServicesArbiter.parm_producer_start_retry_time = 0;
        }
        
        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;
        
        /// <summary>
        /// The entangled libs
        /// </summary>
        private readonly IIoEntangled<TKey> _entangled;

        /// <summary>
        /// Used to store one datum's worth of decoded trits
        /// </summary>//TODO
        public sbyte[] TritBuffer = new sbyte[IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION + IoTransaction.NUM_TRITS_HASH];
        
        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - (BufferOffset - DatumProvisionLengthMax);

        /// <summary>
        /// Used to control how long we wait for the source before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// The decoded tangle transaction
        /// </summary>
        private static IoTangleTransactionSource<TKey> _nodeServicesProxy;

        /// <summary>
        /// The decoded tangle transaction
        /// </summary>
        private static IoTangleTransactionSource<TKey> _neighborProducer;

        /// <summary>
        /// The transaction broadcaster
        /// </summary>
        public IoChannel<IoTangleTransaction<TKey>> NodeServicesArbiter;

        /// <summary>
        /// The transaction broadcaster
        /// </summary>
        public IoChannel<IoTangleTransaction<TKey>> NeighborServicesArbiter;

        /// <summary>
        /// Crc checker
        /// </summary>
        private readonly Crc32 _crc32 = new Crc32();

        /// <summary>
        /// tps counter
        /// </summary>
        private static readonly IoFpsCounter TotalTpsCounter = new IoFpsCounter(); //TODO send this to the source handle

        /// <summary>
        /// tps counter
        /// </summary>
        private static readonly IoFpsCounter ValueTpsCounter = new IoFpsCounter(10); //TODO send this to the source handle

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 250;

        /// <summary>
        /// The time a consumer will wait for a source to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting    

        /// <summary>
        /// The amount of items that can be ready for production before blocking
        /// </summary>
        [IoParameter]
        public int parm_forward_queue_length = 4;

        /// <summary>
        /// Processes a iri datum
        /// </summary>
        private async Task<State> ProcessProtocolMessage() //TODO error cases
        {
            var newInteropTransactions = new List<IIoTransactionModel<TKey>>();
            var s = Stopwatch.StartNew();
            var t = Stopwatch.StartNew();
            try
            {
                if (!Source.Synced && RequiredSync() && !Source.Synced)
                {
                    return ProcessState;
                }

                var localSync = true;

                var syncFailureThreshold = 2;
                var curSyncFailureCount = syncFailureThreshold;
                
                _logger.Trace($"{TraceDescription} Processing {DatumCount} messages...");
                for (var i = 0; i < DatumCount; i++)
                {                    
                    try
                    {
                        s.Restart();
                        if (ProcessState == State.RSync)
                            ProcessState = State.Consuming;

                        var requiredSync = !localSync && RequiredSync();
                        if (!Source.Synced)
                            return ProcessState;
                        else if (requiredSync)
                        {
                            i = 0;
                            ProcessState = State.RSync;
                            continue;
                        }
                            
                        var interopTx = (IIoTransactionModel<TKey>)_entangled.ModelDecoder.GetTransaction(Buffer, BufferOffset, TritBuffer);
                        interopTx.Uri = Source.SourceUri;

                        //check for pow
                        if (interopTx.Pow < TanglePeer<TKey>.MWM && interopTx.Pow > -TanglePeer<TKey>.MWM)
                        {                                                           
                            ProcessState = State.NoPow;                            

                            if (interopTx.Value < -2779530283277761 || interopTx.Value > 2779530283277761)
                            //|| interopTx.Timestamp <= 0 || interopTx.Timestamp > (interopTx.Timestamp.ToString().Length > 11 ? new DateTimeOffset(DateTime.Now + TimeSpan.FromHours(2)).ToUnixTimeMilliseconds() : new DateTimeOffset(DateTime.Now + TimeSpan.FromHours(2)).ToUnixTimeSeconds())) //TODO config
                            {
                                try
                                {
                                    _logger.Trace($"{TraceDescription} Possible garbage tx detected: ({Id}.{i + 1}/{DatumCount}) pow = `{interopTx.Pow}', " +
                                                  $"imported = `{((IoTangleMessage<TKey>)Previous).DatumFragmentLength}', " +
                                                  $"BytesRead = `{BytesRead}', " +
                                                  $"BufferOffset = `{BufferOffset - DatumProvisionLength}', " +
                                                  $"BytesLeftToProcess = `{BytesLeftToProcess}', " +
                                                  $"DatumFragmentLength = `{DatumFragmentLength}', " +                                                  
                                                  $"PB = `{Source.ProducerBarrier.CurrentCount}', " +
                                                  $"CB = `{Source.ConsumerBarrier.CurrentCount}'");
                                    //_logger.Trace($"({Id}.{DatumCount}) value = `{interopTx.Value}'");
                                    //_logger.Trace($"({Id}.{DatumCount}) pow = `{interopTx.Pow}'");
                                    //_logger.Trace($"({Id}.{DatumCount}) time = `{interopTx.Timestamp}'");
                                    //_logger.Trace($"({Id}.{DatumCount}) hash = `{interopTx.AsTrytes(interopTx.Hash)}'");
                                    //_logger.Trace($"({Id}.{DatumCount}) bundle = `{interopTx.AsTrytes(interopTx.Bundle)}'");
                                    //_logger.Trace($"({Id}.{DatumCount}) address = `{interopTx.AsTrytes(interopTx.Address)}'");
                                }
                                catch { }

                                if (--curSyncFailureCount == 0)
                                {
                                    Source.Synced = false;
                                    localSync = false;
                                    BufferOffset -= (syncFailureThreshold - 1) * DatumSize;
                                    curSyncFailureCount = syncFailureThreshold;                                    
                                }
                            }                            
                            continue;                            
                        }

                        curSyncFailureCount = syncFailureThreshold;

                        //Cheap dup checker
                        if (Source.RecentlyProcessed != null) //TODO, dupchecker should always be available, maybe mock it
                        {
                            var stopwatch = Stopwatch.StartNew();                                                        
                            if (await WasProcessedRecentlyAsync(interopTx.AsTrytes(interopTx.HashBuffer)))
                            {
                                stopwatch.Stop();
                                ProcessState = State.FastDup;                                
                                _logger.Trace($"{TraceDescription} Fast duplicate tx dropped: [{interopTx.AsTrytes(interopTx.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms'");
                                continue;
                            }                            
                        } 
                        
                        //Add tx to be processed
                        newInteropTransactions.Add(interopTx);

                        TotalTpsCounter.Tick();
                        if (interopTx.AddressBuffer.Length != 0 && interopTx.Value != 0)
                        {         
                            ValueTpsCounter.Tick();
                            _logger.Info($"{interopTx.AsTrytes(interopTx.AddressBuffer)}, {(interopTx.Value / 1000000).ToString().PadLeft(13, ' ')} Mi, " +
                                         $"[{interopTx.Pow}w, {s.ElapsedMilliseconds}ms, {DatumCount}f, {ValueTpsCounter.Total}/{TotalTpsCounter.Total}tx, {TotalTpsCounter.Fps():#####}/{ValueTpsCounter.Fps():F1} tps]");                            
                        }                                                
                    }
                    finally
                    {
                        if (Source.Synced && ( 
                                    ProcessState == State.Consuming 
                                 || ProcessState == State.NoPow 
                                 || ProcessState == State.FastDup))                            
                            BufferOffset += DatumSize;                        
                    }                    
                }

                //Relay batch
                if (newInteropTransactions.Count > 0)
                {
                    await ForwardToNeighborAsync(newInteropTransactions);
                    await ForwardToNodeServicesAsync(newInteropTransactions);
                }                

                ProcessState = State.Consumed;
            }
            finally
            {
                if (ProcessState != State.Consumed && ProcessState != State.Syncing)
                    ProcessState = State.ConsumeErr;
                t.Stop();
                _logger.Trace($"{TraceDescription} Deserializing `{DatumCount}' messages took `{t.ElapsedMilliseconds}ms', `{DatumCount*1000/(t.ElapsedMilliseconds+1)} m/s'");
            }

            return ProcessState;
        }

        private async Task ForwardToNodeServicesAsync(List<IIoTransactionModel<TKey>> newInteropTransactions)
        {
            //cog the source
            await _nodeServicesProxy.ProduceAsync(source =>
            {                
                ((IoTangleTransactionSource<TKey>) source).TxQueue.TryAdd(newInteropTransactions);
                return Task.FromResult(true);
            });

            //forward transactions
            if (!await NodeServicesArbiter.ProduceAsync( sleepOnConsumerLag: false))
            {
                _logger.Warn($"{TraceDescription} Failed to forward to `{NodeServicesArbiter.Source.Description}'");
            }
        }

        private async Task ForwardToNeighborAsync(List<IIoTransactionModel<TKey>> newInteropTransactions)
        {
            //cog the source
            await _neighborProducer.ProduceAsync(source =>
            {
                if (_neighborProducer.Channel.IsArbitrating) //TODO: For now, We don't want to block when neighbors cant process transactions
                    ((IoTangleTransactionSource<TKey>)source).TxQueue.Add(newInteropTransactions);
                else
                    ((IoTangleTransactionSource<TKey>)source).TxQueue.TryAdd(newInteropTransactions);

                return Task.FromResult(true);
            });

            //forward transactions
            if (!await NeighborServicesArbiter.ProduceAsync())
            {
                _logger.Warn($"{TraceDescription} Failed to forward to `{NeighborServicesArbiter.Source.Description}'");
            }
        }

        /// <summary>
        /// Attempts to synchronize with the protocol byte stream
        /// </summary>
        /// <returns>True if synced achieved, false otherwise</returns>
        private bool RequiredSync()
        {            
            var offset = 0;
            var stopwatch = new Stopwatch();
            var requiredSync = false;
            stopwatch.Start();

            if (!Source.Synced)
            {                
                
                _logger.Debug($"{TraceDescription} Synchronizing `{Source.Description}'...");
                ProcessState = State.Syncing;

                for (var i = 0; i < DatumCount; i++)
                {                    
                    var bytesProcessed = 0;
                    var synced = false;
                    while (bytesProcessed < DatumSize)
                    {
                        synced = true;
                        try
                        {
                            var crc = _crc32.Get(new ArraySegment<byte>((byte[])(Array)Buffer, BufferOffset, Codec.MessageSize)).ToString("x").PadLeft(16, '0');

                            for (var j = Codec.MessageCrcSize; j-- > 0;)
                            {
                                try
                                {
                                    if ((byte)Buffer[BufferOffset + Codec.MessageSize + j] != crc[j])
                                    {                                        
                                        synced = false;
                                        break;
                                    }
                                }
                                catch (Exception)
                                {
                                    _logger.Error($"{TraceDescription} length = `{Buffer.Length}', msgSize = `{Codec.MessageSize}', j = `{j}', t = {BufferOffset + Codec.MessageSize + j}");
                                    synced = false;
                                    break;
                                }
                            }
                            if (!synced)
                            {
                                //_logger.Warn($"`{ProducerHandle.Description}' syncing... `{crc}' != `{Encoding.ASCII.GetString((byte[])(Array)Buffer.Skip(BufferOffset + MessageSize).Take(MessageCrcSize).ToArray())}'");
                                BufferOffset += 1; 
                                bytesProcessed += 1;
                                offset += 1;
                                requiredSync = true;
                            }
                            else
                            {
                                stopwatch.Stop();
                                _logger.Trace($"{TraceDescription} Synchronized stream `{Source.Description}', crc32 = `{crc}', offset = `{offset}, time = `{stopwatch.ElapsedMilliseconds}ms', cps = `{offset/(stopwatch.ElapsedMilliseconds+1)}'");
                                Source.Synced = synced = true;
                                break;
                            }
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, $"{TraceDescription} Error while trying to sync BufferOffset = `{BufferOffset}', DatumCount = `{DatumCount}', DatumFragmentLength = `{DatumFragmentLength}' , BytesLeftToProcess = `{BytesLeftToProcess}', BytesRead = `{BytesRead}'");                            
                        }
                    }
                    
                    if (synced)
                    {
                        break;
                    }                        
                }

                if (requiredSync)
                {
                    //Set how many datums we have available to process
                    DatumCount = BytesLeftToProcess / DatumSize;
                    DatumFragmentLength = BytesLeftToProcess % DatumSize;

                    //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                    StillHasUnprocessedFragments = DatumFragmentLength > 0;                 
                }

                stopwatch.Stop();
            } 
            
            if (!Source.Synced)
            {
                _logger.Warn($"{TraceDescription} Unable to sync stream `{Source.Description}', scanned = `{offset}', time = `{stopwatch.ElapsedMilliseconds}ms'");
            }
            else if(Source.Synced)
            {
                ProcessState = State.Consuming;
            }
            
            return requiredSync;
        }

        private void TransferPreviousBits()
        {
            if (Previous?.StillHasUnprocessedFragments ?? false)
            {
                var previousJobFragment = (IoMessage<IoTangleMessage<TKey>>)Previous;
                try
                {
                    var bytesToTransfer = previousJobFragment.DatumFragmentLength;                    
                    BufferOffset -= bytesToTransfer;                    
                    DatumProvisionLength -= bytesToTransfer;
                    DatumCount = BytesLeftToProcess / DatumSize;
                    DatumFragmentLength = BytesLeftToProcess % DatumSize;
                    StillHasUnprocessedFragments = DatumFragmentLength > 0;

                    Array.Copy(previousJobFragment.Buffer, previousJobFragment.BufferOffset, Buffer, BufferOffset, bytesToTransfer);
                }
                catch (Exception e) // we de-synced 
                {
                    _logger.Warn(e, $"{TraceDescription} We desynced!:");

                    Source.Synced = false;
                    DatumCount = 0;
                    BytesRead = 0;
                    ProcessState = State.Consumed;
                    DatumFragmentLength = 0;
                    StillHasUnprocessedFragments = false;                    
                }
            }
            
        }

        /// <inheritdoc />
        /// <summary>
        /// Manages the barrier between the consumer and the source
        /// </summary>
        /// <returns>The <see cref="F:zero.core.patterns.bushes.IoWorkStateTransition`1.State" /> of the barrier's outcome</returns>
        public override async Task<State> ConsumeAsync()
        {
            TransferPreviousBits();
            
            return await ProcessProtocolMessage(); 
        }

        /// <inheritdoc />
        /// <summary>
        /// Prepares the work to be done from the <see cref="F:erebros.core.patterns.bushes.IoProducable`1.Source" />
        /// </summary>
        /// <returns>The resulting status</returns>
        public override async Task<State> ProduceAsync()
        {
            try
            {
                // We run this piece of code inside this callback so that the source can do some error detections on itself on our behalf
                var sourceTaskSuccess = await Source.ProduceAsync(async ioSocket =>
                {
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    _producerStopwatch.Restart();
                    if (!await Source.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, Spinners.Token))
                    {
                        if (!Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.ProduceTo;
                            _producerStopwatch.Stop();
                            _logger.Warn($"{TraceDescription} timed out waiting for CONSUMER to release, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_producer_wait_for_consumer_timeout}ms', " +
                                         $"CB = `{Source.ConsumerBarrier.CurrentCount}'");

                            //TODO finish when config is fixed
                            //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                            //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) State.Consumed]) /
                            //         (Interlocked.Read(ref Source.Counters[(int) State.Consumed]) * 2 + 1));                                                                    
                        }
                        else
                            ProcessState = State.ProdCancel;
                        return true;
                    }

                    if (Spinners.IsCancellationRequested)
                    {
                        ProcessState = State.ProdCancel;
                        return false;
                    }

                    //Async read the message from the message stream
                    if (Source.IsOperational)
                    {                                                
                        await ((IoSocket)ioSocket).ReadAsync((byte[])(Array)Buffer, BufferOffset, BufferSize).ContinueWith(
                            rx =>
                            {                                                                    
                                switch (rx.Status)
                                {
                                    //Canceled
                                    case TaskStatus.Canceled:
                                    case TaskStatus.Faulted:
                                        ProcessState = rx.Status == TaskStatus.Canceled ? State.ProdCancel : State.ProduceErr;
                                        Source.Zero();
                                        _logger.Error(rx.Exception?.InnerException, $"{TraceDescription} ReadAsync from stream returned with errors:");
                                        break;
                                    //Success
                                    case TaskStatus.RanToCompletion:
                                        var bytesRead = rx.Result;
                                        BytesRead = bytesRead;

                                        //TODO double check this hack
                                        if (BytesRead == 0)
                                        {
                                            ProcessState = State.ProStarting;
                                            DatumFragmentLength = 0;
                                            break;
                                        }

                                        if (Id == 0 && Source is IoTcpClient<IoTangleMessage<TKey>>)
                                        {                                                                  
                                            _logger.Info($"{TraceDescription} Got receiver port as: `{Encoding.ASCII.GetString((byte[])(Array)Buffer).Substring(BufferOffset, 10)}'");
                                            BufferOffset += 10;
                                            bytesRead -= 10;
                                            if (BytesLeftToProcess == 0)
                                            {
                                                ProcessState = State.Produced;
                                                DatumFragmentLength = 0;
                                                break;
                                            }
                                        }
                                        
                                        //Set how many datums we have available to process
                                        DatumCount = BytesLeftToProcess / DatumSize;
                                        DatumFragmentLength = BytesLeftToProcess % DatumSize;

                                        //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                                        StillHasUnprocessedFragments = DatumFragmentLength > 0;

                                        ProcessState = State.Produced;

                                        _logger.Trace($"{TraceDescription} RX=> read=`{bytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLength}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLength) * 100)}%'");

                                        break;
                                    default:
                                        ProcessState = State.ProduceErr;
                                        throw new InvalidAsynchronousStateException($"Job =`{Description}', State={rx.Status}");
                                }
                            }, Spinners.Token);
                    }
                    else
                    {
                        Source.Zero();
                    }

                    if (Spinners.IsCancellationRequested)
                    {
                        ProcessState = State.Cancelled;
                        return false;
                    }
                    return true;
                });

                if (!sourceTaskSuccess)
                {
                    _logger.Trace($"{TraceDescription} Failed to source job");
                }
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"{TraceDescription} Producing job returned with errors:");
            }
            finally
            {
                if (ProcessState == State.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    ProcessState = State.ProduceErr;
                }
            }
            return ProcessState;
        }        
    }
}
