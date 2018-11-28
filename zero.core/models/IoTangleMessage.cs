using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using zero.core.patterns.bushes;
using NLog;
using Tangle.Net.Entity;
using zero.core.conf;
using zero.core.network.ip;
using zero.core.ternary;

namespace zero.core.models
{
    /// <summary>
    /// Specializes a generic <see cref="IoMessage{TProducer}"/> into a specific one for the tangle. This class contains details of how a message is to be 
    /// extracted from <see cref="IoMessage{TProducer}"/>
    /// </summary>
    public class IoTangleMessage : IoMessage<IoTangleMessage>
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructs buffers that hold tangle message information
        /// </summary>  
        /// <param name="source">The network source where messages are to be obtained</param>
        public IoTangleMessage(IoNetClient<IoTangleMessage> source)
        {            
            Producer = source;
            
            DatumLength = MessageLength + ((Producer is IoTcpClient<IoTangleMessage>) ? MessageCrcLength : 0);
            DatumProvisionLength = DatumLength - 1;

            BufferSize = DatumLength * parm_datums_per_buffer;
            Buffer = new sbyte[BufferSize + DatumProvisionLength];

            WorkDescription = source.AddressString;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public sealed override IoProducer<IoTangleMessage> Producer { get; protected set; }

        /// <summary>
        /// Used to store one datum's worth of decoded trits
        /// </summary>
        public sbyte[] TritBuffer = new sbyte[TransactionSize * Codec.TritsPerByte - 1];

        /// <summary>
        /// Used to store one datum's worth of decoded trytes
        /// </summary>
        public StringBuilder TryteBuffer = new StringBuilder((TransactionSize * Codec.TritsPerByte - 1)/3);

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - BufferOffset + DatumProvisionLength;

        /// <summary>
        /// The length of tangle protocol messages
        /// </summary>
        public const int MessageLength = 1650;

        /// <summary>
        /// The size of tangle protocol messages crc
        /// </summary>
        public const int MessageCrcLength = 16;

        /// <summary>
        /// Transaction size
        /// </summary>
        public const int TransactionSize = 1604;

        /// <summary>
        /// Size of the transaction hash
        /// </summary>
        public const int TransactionHashSize = 46;


        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 50;

        /// <summary>
        /// The time a consumer will wait for a producer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 500; //TODO make this adapting

        /// <summary>
        /// The time a producer will wait for a consumer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_consumer_wait_for_producer_timeout = 500;
        
        /// <summary>
        /// Used to control how long we wait for the producer before we report it
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// Processes a iri datum
        /// </summary>
        private void ProcessProtocolMessage()
        {
            var s = new Stopwatch();
            s.Start();
            for (int i = 0; i < DatumCount; i++)
            {
                s.Restart();
                Codec.GetTrits(Buffer, BufferOffset, TritBuffer, IoTangleMessage.TransactionSize);
                Codec.GetTrytes(TritBuffer, 0, TryteBuffer, TritBuffer.Length);

                var tx = Transaction.FromTrytes(new TransactionTrytes(TryteBuffer.ToString()));
                s.Stop();

                //if (tx.Value != 0 && tx.Value < 9999999999999999 && tx.Value > -9999999999999999)
                _logger.Info($"({Id}) {tx.Address}, v={(tx.Value / 1000000).ToString().PadLeft(13, ' ')} Mi, f=`{DatumFragmentLength != 0}', t=`{s.ElapsedMilliseconds}ms'");

                BufferOffset += DatumLength;
            }

            ProcessState = State.Consumed;
        }

        /// <inheritdoc />
        /// <summary>
        /// Manages the barrier between the consumer and the producer
        /// </summary>
        /// <returns>The <see cref="F:zero.core.patterns.bushes.IoWorkStateTransition`1.State" /> of the barrier's outcome</returns>
        public override async Task<State> ConsumeAsync()
        {
            ProcessState = State.Consuming;

            //TODO Find a more elegant way for this terrible hack
            //Discard the neighbor port data
            

            //Process protocol messages
            ProcessProtocolMessage();

            //_logger.Info($"Processed `{message.DatumCount}' datums, remainder = `{message.DatumFragmentLength}', message.BytesRead = `{message.BytesRead}'," +
            //             $" prevJob.BytesLeftToProcess =`{previousJobFragment?.BytesLeftToProcess}'");
            

            return ProcessState;
        }

        /// <inheritdoc />
        /// <summary>
        /// Prepares the work to be done from the <see cref="F:erebros.core.patterns.bushes.IoProducable`1.Source" />
        /// </summary>
        /// <returns>The resulting status</returns>
        public override async Task<State> ProduceAsync(IoProducable<IoTangleMessage> fragment)        
        {
            ProcessState = State.Producing;
            var previousJobFragment = (IoMessage<IoTangleMessage>)fragment;
            try
            {
                // We run this piece of code inside this callback so that the source can do some error detections on itself on our behalf
                await Producer.Execute(async ioSocket =>
                {
                    //TODO maybe we squash datum fragmentation here? But it will double responsiveness.
                    //while (BytesRead < MaxBufferSize && !CancellationTokenSource.IsCancellationRequested)
                    {
                        //----------------------------------------------------------------------------
                        // BARRIER
                        // We are only allowed to run ahead of the consumer by some configurable
                        // amount of steps. Instead of say just filling up memory buffers.
                        // This allows us some kind of (anti DOS?) congestion control
                        //----------------------------------------------------------------------------
                        _producerStopwatch.Restart();
                        if (!await Producer.ProducerBarrier.WaitAsync(parm_producer_wait_for_consumer_timeout, Producer.Spinners.Token))
                        {                            
                            if (!Producer.Spinners.IsCancellationRequested)
                            {                                
                                ProcessState = State.ConsumeTo;
                                _producerStopwatch.Stop();
                                _logger.Warn($"`{Description}' timed out waiting for CONSUMER to release, Waited = `{_producerStopwatch.ElapsedMilliseconds}ms', Willing = `{parm_consumer_wait_for_producer_timeout}ms'");

                                //TODO finish when config is fixed
                                //LocalConfigBus.AddOrUpdate(nameof(parm_consumer_wait_for_producer_timeout), a=>0, 
                                //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) State.Consumed]) /
                                //         (Interlocked.Read(ref Source.Counters[(int) State.Consumed]) * 2 + 1));                                                                    
                            }
                            else
                                ProcessState = State.ConsumeCancelled;
                            return Task.CompletedTask;
                        }

                        if (Producer.Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.ConsumeCancelled;
                            return Task.CompletedTask;
                        }
                            
                        //Async read the message from the message stream
                        await ((IoSocket)ioSocket).ReadAsync((byte [])(Array)Buffer, BufferOffset, BufferSize).ContinueWith(
                        rx =>
                        {
                            switch (rx.Status)
                            {
                                //Canceled
                                case TaskStatus.Canceled:                                            
                                case TaskStatus.Faulted:
                                    ProcessState = rx.Status == TaskStatus.Canceled? State.ProduceCancelled: State.ProduceErr;
                                    Producer.Spinners.Cancel();
                                    Producer.Close();
                                    _logger.Error(rx.Exception?.InnerException, $"ReadAsync from stream `{Description}' returned with errors:");
                                    break;
                                //Success
                                case TaskStatus.RanToCompletion:
                                    var bytesRead = rx.Result;
                                    BytesRead = bytesRead;

                                    //TODO double check this hack
                                    if (BytesRead == 0)
                                    {
                                        ProcessState = State.ProduceSkipped;
                                        break;
                                    }
                                        

                                    if (Id == 0 && Producer is IoTcpClient<IoTangleMessage>)
                                    {
                                        _logger.Info($"Got receiver port as: `{Encoding.ASCII.GetString((byte[])(Array)Buffer).Substring(BufferOffset, 10)}'");
                                        BufferOffset += 10;
                                        BytesRead -= 10;
                                        bytesRead -= 10;

                                        if (BytesLeftToProcess < 0)
                                        {
                                            ProcessState = State.Produced;
                                            break;
                                        }                                            
                                    }

                                    //TODO remove this hack
                                    //Terrible sync hack until we can troll the data for sync later
                                    if (((IoNetClient<IoTangleMessage>)Producer).TcpSynced || (!((IoNetClient<IoTangleMessage>)Producer).TcpSynced && ((BytesLeftToProcess % DatumLength) == 0)))
                                    {
                                        ((IoNetClient<IoTangleMessage>)Producer).TcpSynced = true;
                                    }
                                    else
                                    {
                                        DatumCount = 0;
                                        ProcessState = State.Produced;
                                        _logger.Warn("Syncing...");
                                        break;
                                    }

                                    //Copy a previously read job buffer datum fragment into the current job buffer
                                    if (previousJobFragment != null)
                                    {
                                        BufferOffset -= previousJobFragment.DatumFragmentLength;
                                        BytesRead += previousJobFragment.DatumFragmentLength;
                                        DatumProvisionLength -= previousJobFragment.DatumFragmentLength;
                                        Array.Copy(previousJobFragment.Buffer, previousJobFragment.BufferOffset, Buffer, BufferOffset, previousJobFragment.DatumFragmentLength);
                                    
                                        //Update buffer pointers                                        
                                    }

                                    //Set how many datums we have available to process
                                    DatumCount = BytesLeftToProcess / DatumLength;
                                    DatumFragmentLength = BytesLeftToProcess % DatumLength;
                                    
                                    //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
                                    StillHasUnprocessedFragments = DatumFragmentLength > 0;

                                    ProcessState = State.Produced;
                                    
                                    _logger.Trace($"({Id}) RX=> fragment=`{previousJobFragment?.DatumFragmentLength ?? 0}', read=`{bytesRead}', ready=`{BytesLeftToProcess}', datumcount=`{DatumCount}', datumsize=`{DatumLength}', fragment=`{DatumFragmentLength}', buffer = `{BytesLeftToProcess}/{BufferSize+DatumProvisionLength}', buf = `{(int)(BytesLeftToProcess / (double)(BufferSize + DatumProvisionLength) * 100)}%'");

                                    break;
                                default:
                                    ProcessState = State.ProduceErr;
                                    throw new InvalidAsynchronousStateException($"Job =`{Description}', State={rx.Status}");
                            }                                                                        
                        }, Producer.Spinners.Token);                        
                        
                        if (Producer.Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.Cancelled;
                            return Task.CompletedTask;
                        }
                    }
                    return Task.CompletedTask;
                });
            }
            catch (Exception e)
            {                
                _logger.Warn(e.InnerException ?? e, $"Producing job `{Description}' returned with errors:");
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
            DatumFragmentLength += BytesLeftToProcess;           
        }
    }
}
