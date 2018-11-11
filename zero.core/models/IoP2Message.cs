using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.bushes;
using zero.core.patterns.heap;
using NLog;
using zero.core.conf;
using zero.core.network.ip;

namespace zero.core.models
{
    /// <summary>
    /// Specialises a generic <see cref="IoMessage{TSource}"/> into a spesfic one. This class contains details of how a message is to be 
    /// extracted from <see cref="IoMessage{TSource}"/>
    /// </summary>
    public class IoP2Message : IoMessage<IoNetClient>
    {
        /// <summary>
        /// Constructor
        /// </summary>  
        /// <param name="source">The source of this message</param>
        /// <param name="datumLength"></param>
        public IoP2Message(IoNetClient source, int datumLength):base(datumLength)
        {            
            Source = source;

            WorkDescription = source.Address;
            _logger = LogManager.GetCurrentClassLogger();
            
            //TODO make hot
            _producerTimeout = TimeSpan.FromMilliseconds(parm_lockstep_produce_timeout);

            SettingChangedEvent += (sender, pair) =>
            {
                if (pair.Key == nameof(parm_lockstep_produce_timeout))
                {
                    parm_lockstep_produce_timeout = (long) pair.Value;
                    _producerTimeout = TimeSpan.FromMilliseconds(parm_lockstep_produce_timeout);
                }                    
            };
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        private readonly int _datumLength;

        /// <summary>
        /// The time a consumer will wait for a producer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_lockstep_consume_timeout = 120000; //TODO make this adapting

        /// <summary>
        /// The time a producer will wait for a consumer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_lockstep_produce_timeout = 0;

        /// <summary>
        /// How long the producer is willing to wait for the previous consumer before reporting that it is slowing production down.
        /// </summary>
        private TimeSpan _producerTimeout; 
        
        /// <summary>
        /// Manages the barrier between the consumer and the producer
        /// </summary>
        /// <returns>The <see cref="IoWorkStateTransition{TSource}.State"/> of the barrier's outcome</returns>
        public override State ConsumeBarrier()
        {
            try
            {
                //----------------------------------------------------------------------------
                // PRODUCER BARRIER (we wait for the producer to finish before we consume)
                //----------------------------------------------------------------------------
                if (!Source.ProduceSemaphore.WaitOne((int) parm_lockstep_consume_timeout))
                {
                    var produceState = ProcessState;

                    ProcessState = State.ProduceTo;

                    if (produceState < State.Error && !Source.Spinners.IsCancellationRequested)
                        ProcessState = State.ConsumeCancelled;
                    else
                        ProcessState = State.ConsumerSkipped;
                }
                
                if (ProcessState < State.Error && Source.Spinners.IsCancellationRequested)
                    ProcessState = State.ConsumeCancelled;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Cosuming Job `{Description}' returned with errors:");
                ProcessState = State.ConsumeErr;
            }
            finally
            {
                //----------------------------------------------------------------------------------------
                // RELEASE CONSUMER BARRIER (our producer has finished release the next one that is waiting at the gate)
                //----------------------------------------------------------------------------------------
                Source.ConsumeSemaphore.Release(1);
            }
            
            return ProcessState;
        }

        /// <summary>
        /// Prepares the work to be done from the <see cref="F:erebros.core.patterns.bushes.IoProducable`1.Source" />
        /// </summary>
        /// <returns>The resulting status</returns>
        public override async Task<State> ProduceAsync()
        {
            //while we have not received all the data we expect, read more from the network client
            var success = false;

            try
            {
                // We run this piece of code inside this execute callback so that the lower layers can do some TCP error detections when failures are detected
                success = await Source.Execute(async ioSocket =>
                {
                    //TODO
                    //while (BytesRead < MaxBufferSize && !CancellationTokenSource.IsCancellationRequested)
                    {
                        
                        //----------------------------------------------------------------------------
                        // CONSUMER BARRIER (We cannot process the stream in parallel. Therefor, we 
                        // wait for a consumer to release us as soon as its producer has released it )
                        //----------------------------------------------------------------------------
                        var stopwatch = Stopwatch.StartNew();
                        if (!await Source.ConsumeSemaphore.WaitAsync(_producerTimeout,Source.Spinners.Token))
                        {                            
                            if (!Source.Spinners.IsCancellationRequested)
                            {                                
                                ProcessState = State.ConsumeTo;
                                stopwatch.Stop();
                                _logger.Warn($"`{Description}' timed out waiting for CONSUMER to release, Waited = `{stopwatch.ElapsedMilliseconds}ms', Willing = `{parm_lockstep_produce_timeout}ms'");

                                //LocalConfigBus.AddOrUpdate(nameof(parm_lockstep_produce_timeout), a=>0, 
                                //    (k,v) => Interlocked.Read(ref Source.ServiceTimes[(int) State.Consumed]) /
                                //         (Interlocked.Read(ref Source.Counters[(int) State.Consumed]) * 2 + 1));                                                                    
                            }
                            else
                                ProcessState = State.ConsumeCancelled;
                            return false;
                        }

                        if (Source.Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.ConsumeCancelled;
                            return false;
                        }
                            
                        //Async read the message from the message stream
                        await ioSocket.ReadAsync(this).ContinueWith(
                                rx =>
                                {
                                    switch (rx.Status)
                                    {
                                        //Canceled
                                        case TaskStatus.Canceled:                                            
                                        case TaskStatus.Faulted:
                                            ProcessState = rx.Status == TaskStatus.Canceled? State.ProduceCancelled: State.ProduceErr;
                                        //Faulted                                        
                                            Source.Spinners.Cancel();

                                            ioSocket.Close();
                                            
                                            _logger.Error(rx.Exception?.InnerException, $"ReadAsync from stream `{Description}' was faulted:");

                                            //Release the other side so that it can detect the failure
                                            if (!Source.ProduceSemaphore.SafeWaitHandle.IsClosed)
                                                Source.ProduceSemaphore.Release(1);
                                            break;
                                        //Success
                                        case TaskStatus.RanToCompletion:
                                            ProcessState = State.Produced;
                                            //----------------------------------------------------------------------------------------
                                            // RELEASE THE CONSUMER, work is ready
                                            //----------------------------------------------------------------------------------------
                                            Source.ProduceSemaphore.Release(1);

                                            _logger.Trace($"({Id}) Filled {BytesRead}/{MaxRecvBufSize} ({(int)(BytesRead / (double)MaxRecvBufSize * 100)}%)");

                                            break;
                                        default:
                                        //case TaskStatus.Created:
                                        //case TaskStatus.Running:
                                        //case TaskStatus.WaitingForActivation:
                                        //case TaskStatus.WaitingForChildrenToComplete:
                                        //case TaskStatus.WaitingToRun:
                                            ProcessState = State.ProduceErr;
                                            throw new InvalidAsynchronousStateException(
                                                $"Job =`{Description}', State={rx.Status}");
                                    }                                                                        
                                }, Source.Spinners.Token);                        
                        
                        if (Source.Spinners.IsCancellationRequested)
                        {
                            ProcessState = State.Cancelled;
                            return false;
                        }

                        return true;
                    }
                });
            }
            catch (Exception e)
            {
                success = false;
                _logger.Warn(e.InnerException ?? e, $"Producing job `{Description}' returned with errors:");
            }
            finally
            {                
                if (!success && ProcessState == State.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    ProcessState = State.ProduceErr;                    
                }                
            }

            return ProcessState;
        }
    }
}
