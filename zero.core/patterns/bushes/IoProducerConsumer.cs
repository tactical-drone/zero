using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Producer Consumer pattern
    /// </summary>    
    /// <typeparam name="TJob">The type of job</typeparam>
    public abstract class IoProducerConsumer<TJob> : IoConfigurable, IObservable<IoConsumable<TJob>>
        where TJob : IIoWorker

    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of the progress</param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocMessage">A callback to malloc individual consumer jobs from the heap</param>
        protected IoProducerConsumer(string description, IoProducer<TJob> source, Func<object, IoConsumable<TJob>> mallocMessage)
        {
            ConfigureProducer(description, source, mallocMessage);

            _logger = LogManager.GetCurrentClassLogger();

            //What to do when certain parameters change
            SettingChangedEvent += (sender, pair) =>
            {
                //update heap to match max Q size
                if (pair.Key == nameof(parm_max_q_size))
                {
                    JobHeap.MaxSize = parm_max_q_size;
                }
            };

            //prepare a multicast subject
            ObservableRouter = this.Publish();
            ObservableRouter.Connect();

            //Configure cancellations
            Spinners = new CancellationTokenSource();
            Spinners.Token.Register(() => ObservableRouter.Connect().Dispose());         

            parm_stats_mod_count += new Random((int) DateTime.Now.Ticks).Next((int) (parm_stats_mod_count/2), parm_stats_mod_count);
        }

        /// <summary>
        /// Configures the producer.
        /// </summary>
        /// <param name="description">A description of the producer</param>
        /// <param name="source">An instance of the producer</param>
        /// <param name="mallocMessage"></param>
        public void ConfigureProducer(string description, IoProducer<TJob> source, Func<object, IoConsumable<TJob>> mallocMessage)
        {
            Description = description;            
            Producer = source;            

            JobHeap = new IoHeapIo<IoConsumable<TJob>>(parm_max_q_size) { Make = mallocMessage };
        }

        /// <summary>
        /// The source of the messages
        /// </summary>
        public IoProducer<TJob> Producer;

        /// <summary>
        /// The job queue
        /// </summary>
        private readonly ConcurrentQueue<IoConsumable<TJob>> _queue = new ConcurrentQueue<IoConsumable<TJob>>();

        /// <summary>
        /// A separate ingress point to the q
        /// </summary>
        private readonly BlockingCollection<IoConsumable<TJob>> _ingressBalancer = new BlockingCollection<IoConsumable<TJob>>();

        /// <summary>
        /// Q signaling
        /// </summary>
        private readonly AutoResetEvent _preInsertEvent = new AutoResetEvent(false);

        /// <summary>
        /// The heap where new consumable meta data is allocated from
        /// </summary>
        public IoHeapIo<IoConsumable<TJob>> JobHeap { get; protected set; }

        /// <summary>
        /// A description of this producer consumer
        /// </summary>
        protected string Description;

        /// <summary>
        /// logger
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Stops this producer consumer
        /// </summary>
        public CancellationTokenSource Spinners { get; }

        /// <summary>
        /// The current observer, there can only be one
        /// </summary>
        private IObserver<IoConsumable<TJob>> _observer;

        /// <summary>
        /// A connectable observer, used to multicast messages
        /// </summary>
        public IConnectableObservable<IoConsumable<TJob>> ObservableRouter { private set; get; }

        /// <summary>
        /// Indicates whether jobs are being processed
        /// </summary>
        public bool IsArbitrating { get; set; } = false;

        /// <summary>
        /// Maintains a handle to a job if fragmentation was detected so that the
        /// producer can marshal fragments into the next production
        /// </summary>
        private volatile ConcurrentDictionary<long, IoConsumable<TJob>>  _previousJobFragment = new ConcurrentDictionary<long, IoConsumable<TJob>>();

        /// <summary>
        /// Maximum amount of producers that can be buffered before we stop production of new jobs
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_max_q_size = 1000;

        /// <summary>
        /// Time to wait for insert before complaining about it
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ingress_timout = 500;

        /// <summary>
        /// Debug output rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_stats_mod_count = 1000;

        /// <summary>
        /// Used to rate limit this queue, in ms. Set to -1 for max rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_frame_time = 10;

        /// <summary>
        /// The amount of time to wait between retries when the producer cannot allocate job management structures
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_error_timeout = 10000;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_consumer_threads = 2;

        /// <summary>
        /// The time that the producer will delay waiting for the consumer to free up job buffer space
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_consumer_throttle_delay = 100;

        /// <summary>
        /// The time a producer will wait for a consumer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_consumer_wait_for_producer_timeout = 5000;


        /// <summary>
        /// How long a producer will sleep for when it is getting skipped productions
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_start_retry_time = 1000;

        /// <summary>
        /// Produces the inline instead of in a spin loop
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="sleepOnConsumerLag">if set to <c>true</c> [sleep on consumer lag].</param>
        /// <returns></returns>
        public async Task<bool> ProduceAsync(CancellationToken cancellationToken, bool sleepOnConsumerLag = true) //TODO fix sleepOnConsumerLag variable name that has double meaning
        {
            try
            {
                _logger.Trace($"{nameof(ProduceAsync)}: `{Description}' [ENTER]");
                //And the consumer is keeping up
                if (_queue.Count < parm_max_q_size)
                {
                    IoConsumable<TJob> nextJob = null;
                    bool jobSafeReleased = false;
                    try
                    {
                        //Allocate a job from the heap
                        if ((nextJob = JobHeap.Take()) != null)
                        {
                            while (nextJob.Producer.BlockOnProduceAheadBarrier)
                            {
                                if (!await nextJob.Producer.ProduceAheadBarrier.WaitAsync(-1, Spinners.Token))
                                {
                                    Interlocked.Increment(ref nextJob.Producer.NextProducerId);
                                    return false;
                                }

                                var nextProducerId = Interlocked.Read(ref nextJob.Producer.NextProducerId);
                                if (nextJob.Id == nextProducerId)
                                {
                                    Interlocked.Increment(ref nextJob.Producer.NextProducerId);
                                    break;
                                }
                                _logger.Warn($"{nextJob.TraceDescription} Next id = `{nextJob.Id}' is not {nextProducerId}!!");
                                nextJob.Producer.ProduceAheadBarrier.Release();
                            }

                            //Fetch a job from TProducer. Did we get one?
                            _previousJobFragment.TryRemove(nextJob.Id - 1,  out var prevJobFragment);
                            nextJob.Previous = prevJobFragment;
                            nextJob.ProcessState = IoProducible<TJob>.State.Producing;
                            if (await nextJob.ProduceAsync() < IoProducible<TJob>.State.Error)
                            {                            
                                //TODO Double check this hack
                                //Basically to handle this weird double connection business on the TCP iri side
                                //if (nextJob.ProcessState == IoProducible<TJob>.State.ProStarting)
                                //{
                                //    nextJob.ProcessState = IoProducible<TJob>.State.Produced;

                                //    if (sleepOnConsumerLag && nextJob.Producer.Synced)
                                //        await Task.Delay(parm_producer_start_retry_time, cancellationToken);

                                //    Producer.ProducerBarrier.Release(1);
                                //    return true;
                                //}
                            
                                _previousJobFragment.TryAdd(nextJob.Id, nextJob);

                                try
                                {
                                    if (nextJob.Producer.BlockOnProduceAheadBarrier)
                                        nextJob.Producer.ProduceAheadBarrier.Release();                                
                                }
                                catch
                                {
                                    // ignored
                                }

                                //Enqueue the job for the consumer
                                nextJob.ProcessState = IoProducible<TJob>.State.Queued;
                            
                                //if (!_queue.Contains(nextJob))
                                {
                                    jobSafeReleased = true;
                                    _queue.Enqueue(nextJob);
                                }                                
                                //else
                                //{
                                //    _logger.Fatal($"({nextJob.Id}) is already queued!");   
                                //}                            
                            
                                //Signal to the consumer that there is work to do
                                try
                                {                                
                                    Producer.ConsumerBarrier.Release(1);
                                }
                                catch
                                {
                                    // ignored
                                }                            
                            }
                            else //produce job returned with errors
                            {                            
                                if (nextJob.Producer.BlockOnProduceAheadBarrier)
                                    nextJob.Producer.ProduceAheadBarrier.Release();

                                Producer.ProducerBarrier.Release(1);

                                if (nextJob.ProcessState == IoProducible<TJob>.State.Cancelled ||
                                    nextJob.ProcessState == IoProducible<TJob>.State.ProdCancel)
                                {
                                    Spinners.Cancel();
                                    _logger.Debug($"{nextJob.TraceDescription} Producer `{Description}' is shutting down");
                                    return false;
                                }
                            
                                jobSafeReleased = true;
                                if (nextJob.Previous != null)
                                    _previousJobFragment.TryAdd(nextJob.Previous.Id + 1, (IoConsumable<TJob>) nextJob.Previous);

                                JobHeap.Return(nextJob);                            
                            }
                        }
                        else
                        {                        
                            _logger.Fatal($"Production for: `{Description}` failed. Cannot allocate job resources!");
                            await Task.Delay(parm_error_timeout, Spinners.Token);
                        }
                    }                    
                    catch (ObjectDisposedException) { }
                    catch (TimeoutException) { }
                    catch (OperationCanceledException e)
                    {
                        _logger.Trace(e, $"Producing `{Description}' was cancelled:");
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Producing `{Description}' returned with errors:");
                    }
                    finally
                    {
                        //prevent leaks
                        if (nextJob != null && !jobSafeReleased)
                        {
                            _logger.Warn("Job resources were not freed. BUG!");
                            //TODO Double check this hack
                            if (nextJob.ProcessState != IoProducible<TJob>.State.Finished)
                                nextJob.ProcessState = IoProducible<TJob>.State.Reject;

                            Free(nextJob);                        
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (sleepOnConsumerLag)
                    {
                        _logger.Warn($"Producer for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                        await Task.Delay(parm_producer_consumer_throttle_delay, Spinners.Token);
                    }
                }                
            }
            catch(Exception e)
            {
                _logger.Fatal(e, $"{Description}: ");
            }
            finally
            {
                _logger.Trace($"{nameof(ProduceAsync)}: `{Description}' [EXIT]");
            }

            return true;
        }

        private void Free(IoConsumable<TJob> curJob)
        {            
            if (curJob.Previous != null)
            {
                //if (_queue.Contains(curJob.Previous))
                //{
                //    _logger.Fatal($"Cannot remove job id = `{curJob.Previous.Id}' that is still queued!");
                //    return;
                //}

                JobHeap.Return((IoConsumable<TJob>)curJob.Previous);
            }
                
        }
       
        /// <summary>
        /// Consumes the inline instead of from a spin loop
        /// </summary>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="sleepOnProducerLag">if set to <c>true</c> [sleep on producer lag].</param>
        /// <returns></returns>
        public async Task<Task> ConsumeAsync(Func<IoConsumable<TJob>,Task> inlineCallback = null, bool sleepOnProducerLag = true)
        {
            try
            {
                if (Producer == null)
                    return Task.CompletedTask;

                _logger.Trace($"{nameof(ConsumeAsync)}: `{Description}' [ENTER]");

                if (Producer.BlockOnConsumeAheadBarrier && !await Producer.ConsumeAheadBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout,
                    Spinners.Token))
                {
                    //Was shutdown requested?
                    if (Spinners.IsCancellationRequested)
                    {
                        _logger.Debug($"Consumer `{Description}' is shutting down");
                        return Task.CompletedTask;
                    }

                    //Production timed out
                    if (sleepOnProducerLag)
                    {
                        _logger.Warn($"Consumer `{Description}' [[ReadAheadBarrier]] timed out waiting on `{JobHeap.Make(null).JobDescription}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                    }

                    //Try again                    
                    return Task.CompletedTask;
                }

                //Waiting for a job to be produced. Did production fail?
                if (!await Producer.ConsumerBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, Spinners.Token))
                {
                    //Was shutdown requested?
                    if (Spinners.IsCancellationRequested)
                    {
                        _logger.Debug($"Consumer `{Description}' is shutting down");
                        return Task.CompletedTask;
                    }
                    
                    //Production timed out
                    if (sleepOnProducerLag)
                    {                        
                        _logger.Warn($"Consumer `{Description}' [[ConsumerBarrier]] timed out waiting on `{JobHeap.Make(null).JobDescription}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                        await Task.Delay(parm_consumer_wait_for_producer_timeout/4);
                    }
                        

                    //Try again
                    return Task.CompletedTask;
                }

                //A job was produced. Dequeue it and process
                if (_queue.TryDequeue(out var curJob))
                {
                    curJob.ProcessState = IoProducible<TJob>.State.Dequeued;
                    curJob.ProcessState = IoProducible<TJob>.State.Consuming;
                    try
                    {
                        //Consume the job
                        if (await curJob.ConsumeAsync() >= IoProducible<TJob>.State.Error)
                        {
                            _logger.Trace($"{curJob.TraceDescription} consuming job: `{curJob.ProductionDescription}' was unsuccessful, state = {curJob.ProcessState}");                            
                        }
                        else
                        {
                            if (curJob.ProcessState == IoProducible<TJob>.State.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob);
                            }

                            //Notify observer
                            _observer?.OnNext(curJob);
                        }                        
                    }
                    catch (ArgumentNullException e)
                    {
                        _logger.Trace(e.InnerException ?? e,
                            $"{curJob.TraceDescription} consuming job: `{curJob.ProductionDescription}' returned with errors:");
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e.InnerException ?? e,
                            $"{curJob.TraceDescription} consuming job: `{curJob.ProductionDescription}' returned with errors:");
                    }
                    finally
                    {
                        if (Producer.BlockOnConsumeAheadBarrier)
                            Producer.ConsumeAheadBarrier.Release();

                        //Consume success?
                        if (curJob.ProcessState < IoProducible<TJob>.State.Error)
                        {                            
                            curJob.ProcessState = IoProducible<TJob>.State.Accept;                                                                    
                        }
                        else //Consume failed?
                        {                            
                            curJob.ProcessState = IoProducible<TJob>.State.Reject;
                        }

                        try
                        {
                            //Signal the producer that it can continue to get more work                        
                            Producer.ProducerBarrier.Release(1);
                        }
                        catch
                        {
                            // ignored
                        }
                        finally
                        {                            
                            if ((curJob.Id % parm_stats_mod_count == 0))
                            {
                                _logger.Debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                _logger.Debug($"`{Description}' {JobHeap.IoFpsCounter.Fps():F} j/s, consumer job heap = [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                curJob.Producer.PrintCounters();
                                _logger.Debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                            }

                            Free(curJob);
                        }                                                                        
                    }
                }
                else
                {
                    _logger.Warn($"`{Description}' produced nothing");

                    Producer.ProducerBarrier.Release(1);

                    if (Producer.BlockOnConsumeAheadBarrier)
                        Producer.ConsumeAheadBarrier.Release(1);                    
                }
            }            
            catch (ObjectDisposedException) { }
            catch (TimeoutException) { }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, $"Consumer `{Description}' was cancelled:");
            }
            catch (ArgumentNullException e) {
                _logger.Trace(e, $"Consumer `{Description}' dequeue returned with errors:");
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Consumer `{Description}' dequeue returned with errors:");
            }
            finally
            {
                _logger.Trace($"{nameof(ConsumeAsync)}: `{Description}' [EXIT]");
                //GC.Collect(GC.MaxGeneration);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Starts this producer consumer
        /// </summary>
        /// <param name="cancellationToken">The kill signal</param>
        /// <param name="spawnProducer">Sometimes we don't want to start the producer, for example when we are forwarding to another producer consumer queue</param>
        public virtual async Task SpawnProcessingAsync(CancellationToken cancellationToken, bool spawnProducer = true)
        {
            _logger.Trace($"Starting processing for `{Producer.Description}'");
            using (cancellationToken.Register(() => Spinners.Cancel()))
            {
                //Producer
                var producerTask = Task.Factory.StartNew(async () =>
                {
                    //While not cancellation requested
                    while (!Spinners.IsCancellationRequested && spawnProducer)
                    {                        
                        await ProduceAsync(cancellationToken);
                        if (!Producer.IsOperational)
                        {
                            _logger.Trace($"Producer `{Description}' went non operational!");
                            break;
                        }
                            
                    }
                }, TaskCreationOptions.LongRunning);

                //Consumer
                var consumerTask = Task.Factory.StartNew(async () =>
                {
                    //While supposed to be working
                    while (!Spinners.IsCancellationRequested)
                    {                        
                        await ConsumeAsync();
                        if (!Producer.IsOperational)
                        {
                            _logger.Trace($"Consumer `{Description}' went non operational!");
                            break;
                        }                            
                    }
                }, TaskCreationOptions.LongRunning);

                //Wait for tear down                
                await Task.WhenAll(producerTask.Unwrap(), consumerTask.Unwrap()).ContinueWith(t=>
                {
                    Producer.Close();
                }, cancellationToken);

                _logger.Trace($"Processing for `{Producer.Description}' stopped");
            }

        }

        /// <summary>
        /// Subscribe to this producer, one at a time.
        /// </summary>
        /// <param name="observer">The observer that has to be notified</param>
        /// <returns></returns>
        public IDisposable Subscribe(IObserver<IoConsumable<TJob>> observer)
        {
            _observer = observer;
            return IoAnonymousDisposable.Create(() => { _observer = null; });
        }
    }
}
