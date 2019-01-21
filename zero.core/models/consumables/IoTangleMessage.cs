using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using NLog;
using zero.core.api.controllers.generic;
using zero.core.conf;
using zero.core.core;
using zero.core.misc;
using zero.core.models.consumables.sources;
using zero.core.models.generic;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.protocol;
using zero.interop.entangled;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.interfaces;
using zero.interop.entangled.mock;
using Logger = NLog.Logger;

namespace zero.core.models.consumables
{
    /// <summary>
    /// Specializes a generic <see cref="IoMessage{TProducer}"/> into a specific one for the tangle. This class contains details of how a message is to be 
    /// extracted from <see cref="IoMessage{TProducer}"/>
    /// </summary>
    public class IoTangleMessage<TBlob> : IoMessage<IoTangleMessage<TBlob>> 
    {
        /// <summary>
        /// Constructs buffers that hold tangle message information
        /// </summary>  
        /// <param name="source">The network source where messages are to be obtained</param>
        public IoTangleMessage(IoProducer<IoTangleMessage<TBlob>> source)
        {
            _logger = LogManager.GetCurrentClassLogger();

            _entangled = IoEntangled<TBlob>.Default;

            //Every job knows which source produced it
            ProducerHandle = source;

            //Set some tangle specific protocol constants
            DatumSize = Codec.MessageSize + ((ProducerHandle is IoTcpClient<IoTangleMessage<TBlob>>) ? Codec.MessageCrcSize : 0);
            
            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer;
            DatumProvisionLength = BufferSize * 2;
            Buffer = new sbyte[BufferSize + DatumProvisionLength];

            //Configure a description of this consumer
            WorkDescription = $"Deserialize messages from `{source.Description}'";

            //forward to nodeservices
            if (!ProducerHandle.ObjectStorage.ContainsKey(nameof(_nodeServicesProxy)))
            {
                _nodeServicesProxy = new IoTangleMessageSource<TBlob>($"{nameof(_nodeServicesProxy)}",ProducerHandle);
                if (!ProducerHandle.ObjectStorage.TryAdd(nameof(_nodeServicesProxy), _nodeServicesProxy))
                {
                    _nodeServicesProxy = (IoTangleMessageSource<TBlob>)ProducerHandle.ObjectStorage[nameof(_nodeServicesProxy)];
                }
            }

            NodeServicesRelay = source.GetRelaySource(nameof(IoNodeServices<TBlob>), _nodeServicesProxy, userData => new IoTangleTransaction<TBlob>(_nodeServicesProxy));

            //forward to neighbors
            if (!ProducerHandle.ObjectStorage.ContainsKey(nameof(_neighborProxy)))
            {
                _neighborProxy = new IoTangleMessageSource<TBlob>($"{nameof(_neighborProxy)}", ProducerHandle);
                if (!ProducerHandle.ObjectStorage.TryAdd(nameof(_neighborProxy), _neighborProxy))
                {
                    _neighborProxy = (IoTangleMessageSource<TBlob>)ProducerHandle.ObjectStorage[nameof(_neighborProxy)];
                }
            }

            NeighborRelay = source.GetRelaySource(nameof(IoNeighbor<IoTangleTransaction<TBlob>>), _neighborProxy, userData => new IoTangleTransaction<TBlob>(_neighborProxy));

            //tweak this producer
            NodeServicesRelay.parm_consumer_wait_for_producer_timeout = 0;
            NodeServicesRelay.parm_producer_skipped_delay = 0;

            NeighborRelay.parm_consumer_wait_for_producer_timeout = 5000; //TODO config
            NeighborRelay.parm_producer_skipped_delay = 0;
        }

        public sealed override string ProductionDescription => base.ProductionDescription;

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public sealed override IoProducer<IoTangleMessage<TBlob>> ProducerHandle { get; protected set; }

        /// <summary>
        /// The entangled libs
        /// </summary>
        private readonly IIoEntangled<TBlob> _entangled;

        /// <summary>
        /// Used to store one datum's worth of decoded trits
        /// </summary>//TODO
        public sbyte[] TritBuffer = new sbyte[IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION + IoTransaction.NUM_TRITS_HASH];

        /// <summary>
        /// Used to store one datum's worth of decoded trytes
        /// </summary>
        public StringBuilder TryteBuffer = new StringBuilder((Codec.TransactionSize * Codec.TritsPerByte - 1) / Codec.Radix);

        /// <summary>
        /// The tryte bytebuffer
        /// </summary>
        public sbyte[] TryteByteBuffer = new sbyte[(Codec.TransactionSize * Codec.TritsPerByte - 1) / Codec.Radix];

        /// <summary>
        /// Used to store the hash trits
        /// </summary>//TODO
        public sbyte[] TritHashBuffer = new sbyte[((Codec.TransactionHashSize) * Codec.TritsPerByte) + 1];

        /// <summary>
        /// Used to store the hash trytes
        /// </summary>
        public StringBuilder TryteHashBuffer = new StringBuilder(((Codec.TransactionHashSize) * Codec.TritsPerByte + 1) / Codec.Radix);

        /// <summary>
        /// The tryte hash byte buffer
        /// </summary>
        public sbyte[] TryteHashByteBuffer = new sbyte[(int)Math.Ceiling((decimal)(Codec.TransactionHashSize * Codec.TritsPerByte / Codec.Radix) + 1)]; //TODO where does this +1 come from? Why is it here?

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - BufferOffset + DatumProvisionLength;

        /// <summary>
        /// Used to control how long we wait for the producer before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// The decoded tangle transaction
        /// </summary>
        private static IoTangleMessageSource<TBlob> _nodeServicesProxy;

        /// <summary>
        /// The decoded tangle transaction
        /// </summary>
        private static IoTangleMessageSource<TBlob> _neighborProxy;

        /// <summary>
        /// The transaction broadcaster
        /// </summary>
        public IoForward<IoTangleTransaction<TBlob>> NodeServicesRelay;

        /// <summary>
        /// The transaction broadcaster
        /// </summary>
        public IoForward<IoTangleTransaction<TBlob>> NeighborRelay;

        /// <summary>
        /// Crc checker
        /// </summary>
        private Crc32 _crc32 = new Crc32();

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 200;

        /// <summary>
        /// The time a consumer will wait for a producer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting
        
        /// <summary>
        /// Processes a iri datum
        /// </summary>
        private async Task ProcessProtocolMessage() //TODO error cases
        {
            var newInteropTransactions = new List<IIoTransactionModel<TBlob>>();
            var s = new Stopwatch();
            s.Start();
            
            try
            {
                if (!ProducerHandle.Synced && !Sync())
                {
                    return;
                }

                var syncFailureThreshold = 2;
                var curSyncFailureCount = syncFailureThreshold;
                
                for (var i = 0; i < DatumCount; i++)
                {                    
                    try
                    {
                        s.Restart();

                        //if (!localSync && !Sync())
                        //    return;

                        var interopTx = _entangled.ModelDecoder.GetTransaction(Buffer, BufferOffset, TritBuffer);
                        interopTx.Uri = ProducerHandle.SourceUri;

                        //check for pow
                        if (interopTx.Pow < TanglePeer<object>.MWM && interopTx.Pow > -TanglePeer<object>.MWM)
                        {                               
                            if( ProcessState == State.Consuming )
                                ProcessState = State.NoPow;                            

                            if (interopTx.Value < -2779530283277761 || interopTx.Value > 2779530283277761)
                            //|| interopTx.Timestamp <= 0 || interopTx.Timestamp > (interopTx.Timestamp.ToString().Length > 11 ? new DateTimeOffset(DateTime.Now + TimeSpan.FromHours(2)).ToUnixTimeMilliseconds() : new DateTimeOffset(DateTime.Now + TimeSpan.FromHours(2)).ToUnixTimeSeconds())) //TODO config
                            {
                                try
                                {
                                    _logger.Trace($"Possible garbage tx detected: ({Id}.{DatumCount}) pow = `{interopTx.Pow}', PB = `{ProducerHandle.ProducerBarrier.CurrentCount}', CB = `{ProducerHandle.ConsumerBarrier.CurrentCount}'");
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
                                    ProducerHandle.Synced = false;                                    
                                    BufferOffset -= (syncFailureThreshold - 1) * DatumSize;
                                    curSyncFailureCount = syncFailureThreshold;                                    
                                }
                            }                            
                            continue;
                        }                        
                        
                        curSyncFailureCount = syncFailureThreshold;
                        
                        newInteropTransactions.Add(interopTx);

                        if (interopTx.Address != null && interopTx.Value != 0 )
                        {
                            _logger.Info($"({Id}) {interopTx.AsTrytes(interopTx.Address, IoTransaction.NUM_TRITS_ADDRESS).PadRight(IoTransaction.NUM_TRYTES_ADDRESS)}, v={(interopTx.Value / 1000000).ToString().PadLeft(13, ' ')} Mi, f=`{DatumFragmentLength != 0}', pow= `{interopTx.Pow}', t= `{s.ElapsedMilliseconds}ms'");                            
                        }                            
                    }
                    finally
                    {
                        if (ProducerHandle.Synced || ProcessState == State.Consuming || ProcessState == State.NoPow)
                        //if (ProcessState == State.Consuming)
                            BufferOffset += DatumSize;
                    }                    
                }

                //Relay batch
                await ForwardToNeighbor(newInteropTransactions);
                await ForwardToNodeServices(newInteropTransactions);

                ProcessState = State.Consumed;
            }
            finally
            {
                if (ProcessState != State.Consumed && ProcessState != State.Syncing)
                    ProcessState = State.ConsumeErr;
            }
        }

        private async Task ForwardToNodeServices(List<IIoTransactionModel<TBlob>> newInteropTransactions)
        {
            //cog the source
            await _nodeServicesProxy.ProduceAsync(source =>
            {
                if (NodeServicesRelay.PrimaryProducer.ProducerBarrier.CurrentCount != 0)
                    ((IoTangleMessageSource<TBlob>) source).TxQueue.Enqueue(newInteropTransactions);

                return Task.FromResult(true);
            });

            //forward transactions
            if (!await NodeServicesRelay.ProduceAsync(ProducerHandle.Spinners.Token, sleepOnConsumerLag: false))
            {
                _logger.Warn($"Failed to relay to `{NodeServicesRelay.PrimaryProducer.Description}'");
            }
        }

        private async Task ForwardToNeighbor(List<IIoTransactionModel<TBlob>> newInteropTransactions)
        {
            //cog the source
            await _neighborProxy.ProduceAsync(source =>
            {
                if (NeighborRelay.PrimaryProducer.ProducerBarrier.CurrentCount != 0)
                    ((IoTangleMessageSource<TBlob>)source).TxQueue.Enqueue(newInteropTransactions);

                return Task.FromResult(true);
            });

            //forward transactions
            if (!await NeighborRelay.ProduceAsync(ProducerHandle.Spinners.Token, sleepOnConsumerLag: true))
            {
                _logger.Warn($"Failed to relay to `{NeighborRelay.PrimaryProducer.Description}'");
            }
        }

        /// <summary>
        /// Attempts to synchronize with the protocol byte stream
        /// </summary>
        /// <returns>True if synced achieved, false otherwise</returns>
        private bool Sync()
        {            
            var offset = 0;
            if (!ProducerHandle.Synced)
            {                
                bool synced = true;
                _logger.Debug($"({Id}) Synchronizing `{ProducerHandle.Description}'...");
                ProcessState = State.Syncing;

                for (var i = 0; i < DatumCount; i++)
                {
                    var requiredSync = false;
                    var bytesLeftToProcess = 0;
                    while (bytesLeftToProcess < DatumSize)
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
                                _logger.Error($"length = `{Buffer.Length}', msgSize = `{Codec.MessageSize}', j = `{j}', t = {BufferOffset + Codec.MessageSize + j}");
                            }
                        }
                        if (!synced)
                        {
                            //_logger.Warn($"`{ProducerHandle.Description}' syncing... `{crc}' != `{Encoding.ASCII.GetString((byte[])(Array)Buffer.Skip(BufferOffset + MessageSize).Take(MessageCrcSize).ToArray())}'");                        
                            BufferOffset++;
                            bytesLeftToProcess++;
                            offset++;
                            requiredSync = true;
                        }
                        else
                        {
                            _logger.Trace($"({Id}) Synchronized stream `{ProducerHandle.Description}', crc32 = `{crc}', offset = `{offset}'");
                            ProducerHandle.Synced = true;
                            break;
                        }
                    }

                    if (synced)
                    {
                        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                        if (requiredSync)
                        {                            
                            var remainingBytesWithoutFragment = BytesLeftToProcess - DatumFragmentLength;
                            if (remainingBytesWithoutFragment > 0)
                                DatumCount = (remainingBytesWithoutFragment) / DatumSize;
                            _logger.Warn($"remaining = {remainingBytesWithoutFragment}, datums = {DatumCount}");
                        }                        
                        break;
                    }                        
                }                
            } 
            
            if (!ProducerHandle.Synced)
            {
                _logger.Warn($"({Id}) Unable to sync stream `{ProducerHandle.Description}', scanned = `{offset}'");
            }
            else if(ProducerHandle.Synced)
            {
                ProcessState = State.Consuming;
            }

            
            return ProducerHandle.Synced;
        }

        /// <inheritdoc />
        /// <summary>
        /// Manages the barrier between the consumer and the producer
        /// </summary>
        /// <returns>The <see cref="F:zero.core.patterns.bushes.IoWorkStateTransition`1.State" /> of the barrier's outcome</returns>
        public override async Task<State> ConsumeAsync()
        {
            //Process protocol messages
            await ProcessProtocolMessage();

            //_logger.Info($"Processed `{message.DatumCount}' datums, remainder = `{message.DatumFragmentLength}', message.BytesRead = `{message.BytesRead}'," +
            //             $" prevJob.BytesLeftToProcess =`{previousJobFragment?.BytesLeftToProcess}'");

            return ProcessState;
        }

        /// <inheritdoc />
        /// <summary>
        /// Prepares the work to be done from the <see cref="F:erebros.core.patterns.bushes.IoProducable`1.Source" />
        /// </summary>
        /// <returns>The resulting status</returns>
        public override async Task<State> ProduceAsync(IoProduceble<IoTangleMessage<TBlob>> fragment)
        {
            ProcessState = State.Producing;
            var previousJobFragment = (IoMessage<IoTangleMessage<TBlob>>)fragment;
            try
            {
                // We run this piece of code inside this callback so that the source can do some error detections on itself on our behalf
                var sourceTaskSuccess = await ProducerHandle.ProduceAsync(async ioSocket =>
                {
                    //----------------------------------------------------------------------------
                    // BARRIER
                    // We are only allowed to run ahead of the consumer by some configurable
                    // amount of steps. Instead of say just filling up memory buffers.
                    // This allows us some kind of (anti DOS?) congestion control
                    //----------------------------------------------------------------------------
                    _producerStopwatch.Restart();
                    if (!await ProducerHandle.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, ProducerHandle.Spinners.Token))
                    {
                        if (!ProducerHandle.Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.ProduceTo;
                            _producerStopwatch.Stop();
                            _logger.Warn($"`{ProductionDescription}' timed out waiting for CONSUMER to release, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_producer_wait_for_consumer_timeout}ms'");

                            //TODO finish when config is fixed
                            //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                            //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) State.Consumed]) /
                            //         (Interlocked.Read(ref Source.Counters[(int) State.Consumed]) * 2 + 1));                                                                    
                        }
                        else
                            ProcessState = State.ProdCancel;
                        return true;
                    }

                    if (ProducerHandle.Spinners.IsCancellationRequested)
                    {
                        ProcessState = State.ProdCancel;
                        return false;
                    }

                    //Async read the message from the message stream
                    if (ProducerHandle.IsOperational)
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
                                        ProducerHandle.Spinners.Cancel();
                                        ProducerHandle.Close();
                                        _logger.Error(rx.Exception?.InnerException, $"ReadAsync from stream `{ProductionDescription}' returned with errors:");
                                        break;
                                    //Success
                                    case TaskStatus.RanToCompletion:
                                        var bytesRead = rx.Result;
                                        BytesRead = bytesRead;

                                        //TODO double check this hack
                                        if (BytesRead == 0)
                                        {
                                            ProcessState = State.ProSkipped;
                                            DatumFragmentLength = 0;
                                            break;
                                        }

                                        if (Id == 0 && ProducerHandle is IoTcpClient<IoTangleMessage<TBlob>>)
                                        {
                                            _logger.Info($"Got receiver port as: `{Encoding.ASCII.GetString((byte[])(Array)Buffer).Substring(BufferOffset, 10)}'");
                                            BufferOffset += 10;
                                            bytesRead -= 10;
                                            if (BytesLeftToProcess == 0)
                                            {
                                                ProcessState = State.Produced;
                                                DatumFragmentLength = 0;
                                                break;
                                            }
                                        }

                                        //Copy a previously read job buffer datum fragment into the current job buffer
                                        if (previousJobFragment != null)
                                        {
                                            try
                                            {
                                                BufferOffset -= Math.Min(previousJobFragment.DatumFragmentLength, DatumProvisionLength);
                                                BytesRead += Math.Min(previousJobFragment.DatumFragmentLength, DatumProvisionLength);
                                                DatumProvisionLength -= Math.Min(previousJobFragment.DatumFragmentLength, DatumProvisionLength);
                                                Array.Copy(previousJobFragment.Buffer, previousJobFragment.BufferOffset, Buffer, BufferOffset, Math.Min(previousJobFragment.DatumFragmentLength, DatumProvisionLength));
                                            }
                                            catch(Exception e) // we de-synced 
                                            {
                                                _logger.Warn(e,"We desynced!:");

                                                ProducerHandle.Synced = false;
                                                DatumCount = 0;
                                                BytesRead = 0;
                                                ProcessState = State.ProSkipped;
                                                DatumFragmentLength = 0;
                                                break;
                                            }
                                            //Update buffer pointers                                        
                                        }

                                        //Set how many datums we have available to process
                                        DatumCount = BytesLeftToProcess / DatumSize;
                                        DatumFragmentLength = BytesLeftToProcess % DatumSize;

                                        //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                                        StillHasUnprocessedFragments = DatumFragmentLength > 0;

                                        ProcessState = State.Produced;

                                        //_logger.Trace($"({Id}) RX=> fragment=`{previousJobFragment?.DatumFragmentLength ?? 0}', read=`{bytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumSize}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize + DatumProvisionLength}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLength) * 100)}%'");

                                        break;
                                    default:
                                        ProcessState = State.ProduceErr;
                                        throw new InvalidAsynchronousStateException($"Job =`{ProductionDescription}', State={rx.Status}");
                                }
                            }, ProducerHandle.Spinners.Token);
                    }
                    else
                    {
                        ProducerHandle.Close();
                    }

                    if (ProducerHandle.Spinners.IsCancellationRequested)
                    {
                        ProcessState = State.Cancelled;
                        return false;
                    }
                    return true;
                });

                if (!sourceTaskSuccess)
                {
                    _logger.Trace($"Failed to source job from `{ProducerHandle.Description}' for `{ProductionDescription}'");
                }
            }
            catch (Exception e)
            {
                _logger.Warn(e, $"Producing job `{ProductionDescription}' returned with errors:");
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


        /// <summary>
        /// Set unprocessed data as more fragments.
        /// </summary>
        public override void MoveUnprocessedToFragment()
        {            
            //Set how many datums we have available to process
            DatumCount = BytesLeftToProcess / DatumSize;
            DatumFragmentLength = BytesLeftToProcess % DatumSize;

            //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
            StillHasUnprocessedFragments = DatumFragmentLength > 0;
        }
    }
}
