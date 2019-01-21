using System;
using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.schedulers;
using zero.core.protocol;

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
        /// <param name="description"></param>
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
                    JobMetaHeap.MaxSize = parm_max_q_size;
                }
            };

            //prepare a multicast subject
            ObservableRouter = this.Publish();
            ObservableRouter.Connect();

            //Configure cancellations
            Spinners = new CancellationTokenSource();
            Spinners.Token.Register(() => ObservableRouter.Connect().Dispose());

            // set a default scheduler
            JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads);
        }

        /// <summary>
        /// Configures the producer.
        /// </summary>
        /// <param name="description">A description of the producer</param>
        /// <param name="source">An instance of the producer</param>
        /// <param name="mallocMessage"></param>
        public void ConfigureProducer(string description, IoProducer<TJob> source, Func<object, IoConsumable<TJob>> mallocMessage)
        {
            PrimaryProducerDescription = description;
            PrimaryProducer = source;

            JobMetaHeap = new IoHeapIo<IoConsumable<TJob>>(parm_max_q_size) { Make = mallocMessage };
        }

        /// <summary>
        /// The source of the messages
        /// </summary>
        public IoProducer<TJob> PrimaryProducer;

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
        public IoHeapIo<IoConsumable<TJob>> JobMetaHeap { get; protected set; }

        /// <summary>
        /// A description of this producer consumer
        /// </summary>
        protected string PrimaryProducerDescription;

        /// <summary>
        /// logger
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Stops this producer consumer
        /// </summary>
        public CancellationTokenSource Spinners;

        /// <summary>
        /// The current observer, there can only be one
        /// </summary>
        private IObserver<IoConsumable<TJob>> _observer;

        /// <summary>
        /// A connectable observer, used to multicast messages
        /// </summary>
        public IConnectableObservable<IoConsumable<TJob>> ObservableRouter { private set; get; }

        /// <summary>
        /// The scheduler used to do work on
        /// </summary>
        protected LimitedThreadScheduler JobThreadScheduler;

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
        public long parm_stats_mod_count = 100;

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
        public int parm_producer_skipped_delay = 1000;

        /// <summary>
        /// Produces the inline instead of in a spin loop
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="sleepOnConsumerLag">if set to <c>true</c> [sleep on consumer lag].</param>
        /// <returns></returns>
        public async Task<bool> ProduceAsync(CancellationToken cancellationToken, bool sleepOnConsumerLag = true)
        {
            //And the consumer is keeping up
            if (_queue.Count < parm_max_q_size)
            {
                IoConsumable<TJob> nextJob = null;
                bool wasQueued = false;
                try
                {
                    //Allocate a job from the heap
                    if ((nextJob = JobMetaHeap.Take()) != null)
                    {
                        //Fetch a job from TProducer. Did we get one?
                        _previousJobFragment.TryRemove(nextJob.Id - 1,  out var prevJobFragment);
                        if (prevJobFragment != null && !prevJobFragment.StillHasUnprocessedFragments)
                            prevJobFragment = null;
                        if (await nextJob.ProduceAsync(prevJobFragment) < IoProduceble<TJob>.State.Error)
                        {
                            //TODO Double check this hack
                            //Basically to handle this weird double connection business on the TCP iri side
                            if (nextJob.ProcessState == IoProduceble<TJob>.State.ProSkipped)
                            {
                                nextJob.ProcessState = IoProduceble<TJob>.State.Accept;

                                if (sleepOnConsumerLag && nextJob.ProducerHandle.Synced)
                                    await Task.Delay(parm_producer_skipped_delay, cancellationToken);

                                PrimaryProducer.ProducerBarrier.Release(1);
                                return true;
                            }

                            //Does production yield fragmented datums?
                            if (nextJob.StillHasUnprocessedFragments)
                                _previousJobFragment.TryAdd(nextJob.Id, nextJob);                                

                            //Enqueue the job for the consumer
                            nextJob.ProcessState = IoProduceble<TJob>.State.Queued;
                            _queue.Enqueue(nextJob);
                            wasQueued = true;

                            //Signal to the consumer that there is work to do
                            try
                            {
                                PrimaryProducer.ConsumerBarrier.Release(1);
                            }
                            catch
                            {
                                // ignored
                            }

                            //Prepare this production's remaining datum fragments for the next production
                            if (prevJobFragment != null)
                            {
                                lock (prevJobFragment)
                                {
                                    //If this job is not under the consumer's control, we need to return it to the heap 
                                    if (prevJobFragment.ProcessState > IoProduceble<TJob>.State.Consumed)
                                        JobMetaHeap.Return(prevJobFragment);
                                    else //Signal control back to the consumer that it now has control over this job
                                        prevJobFragment.StillHasUnprocessedFragments = false;
                                }
                            }                            
                        }
                        else //produce job returned with errors
                        {
                            if (nextJob.ProcessState == IoProduceble<TJob>.State.Cancelled ||
                                nextJob.ProcessState == IoProduceble<TJob>.State.ProdCancel)
                            {
                                Spinners.Cancel();
                                _logger.Debug($"Producer `{PrimaryProducerDescription}' is shutting down");
                                return false;
                            }

                            //TODO double check what we are supposed to do here?
                            //TODO what about the previous fragment?
                            var sleepTimeMs = nextJob.ProducerHandle.Counters[(int) IoProduceble<TJob>.State.Reject] +
                                              1 / (nextJob.ProducerHandle.Counters
                                                       [(int) IoProduceble<TJob>.State.Accept] + parm_error_timeout);

                            if (sleepOnConsumerLag)
                            {
                                _logger.Debug(
                                    $"{nextJob.ProductionDescription}, Reject = {nextJob.ProducerHandle.Counters[(int) IoProduceble<TJob>.State.Reject]}, Accept = {nextJob.ProducerHandle.Counters[(int) IoProduceble<TJob>.State.Accept]}");
                                _logger.Debug(
                                    $"`{PrimaryProducerDescription}' producing job `{nextJob.ProductionDescription}' returned with state `{nextJob.ProcessState}', sleeping for {sleepTimeMs}ms...");

                                try
                                {
                                    await Task.Delay((int) sleepTimeMs, Spinners.Token);
                                }
                                catch
                                {
                                    // ignored
                                }
                            }

                        }
                    }
                    else
                    {
                        //TODO will this ever happen?
                        _logger.Warn(
                            $"Producing for `{PrimaryProducerDescription}` failed. Cannot allocate job resources");
                        await Task.Delay(parm_error_timeout, Spinners.Token);
                    }
                }
                catch (TaskCanceledException e)
                {
                    _logger.Trace(e, $"Producing `{PrimaryProducerDescription}' was cancelled:");
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Producing `{PrimaryProducerDescription}' returned with errors:");
                }
                finally
                {
                    //prevent leaks
                    if (nextJob != null && !wasQueued)
                    {
                        //TODO Double check this hack
                        if (nextJob.ProcessState != IoProduceble<TJob>.State.Finished)
                            nextJob.ProcessState = IoProduceble<TJob>.State.Reject;
                        JobMetaHeap.Return(nextJob);
                    }
                }
            }
            else //We have run out of buffer space. Wait for the consumer to catch up
            {
                if (sleepOnConsumerLag)
                {
                    _logger.Warn($"Producer for `{PrimaryProducerDescription}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                    await Task.Delay(parm_producer_consumer_throttle_delay, Spinners.Token);
                }
            }

            return true;
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
                if (PrimaryProducer == null)
                    return Task.CompletedTask;

                //Waiting for a job to be produced. Did production fail?
                if (!await PrimaryProducer.ConsumerBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, Spinners.Token))
                {
                    //Was shutdown requested?
                    if (Spinners.IsCancellationRequested)
                    {
                        _logger.Debug($"Consumer `{PrimaryProducerDescription}' is shutting down");
                        return Task.CompletedTask;
                    }

                    //Production timed out
                    if (sleepOnProducerLag)
                    {                        
                        _logger.Warn($"Consumer `{PrimaryProducerDescription}' timed out waiting on `{JobMetaHeap.Make(null).JobDescription}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                    }
                        

                    //Try again
                    return Task.CompletedTask;
                }

                //A job was produced. Dequeue it and process
                if (_queue.TryDequeue(out var currJob))
                {
                    currJob.ProcessState = IoProduceble<TJob>.State.Consuming;
                    try
                    {
                        //Consume the job
                        if (await currJob.ConsumeAsync() >= IoProduceble<TJob>.State.Error)
                        {
                            _logger.Trace($"`{PrimaryProducerDescription}' consuming job `{currJob.ProductionDescription}' was unsuccessful, state = {currJob.ProcessState}");
                        }

                        if (currJob.ProcessState == IoProduceble<TJob>.State.ConInlined && inlineCallback != null)
                        {
                            //forward any jobs                                                                             
                            await inlineCallback(currJob);
                        }

                        //Notify observer
                        _observer?.OnNext(currJob);
                    }
                    catch (ArgumentNullException e)
                    {
                        _logger.Trace(e.InnerException ?? e,
                            $"`{PrimaryProducerDescription}' consuming job `{currJob.ProductionDescription}' returned with errors:");
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e.InnerException ?? e,
                            $"`{PrimaryProducerDescription}' consuming job `{currJob.ProductionDescription}' returned with errors:");
                    }
                    finally
                    {                        
                        //Consume success?
                        if (currJob.ProcessState == IoProduceble<TJob>.State.Consumed)
                        {
                            currJob.ProcessState = IoProduceble<TJob>.State.Accept;

                            if (!currJob.StillHasUnprocessedFragments)
                                JobMetaHeap.Return(currJob);                            
                        }
                        else //Consume failed?
                        {                            
                            if (!currJob.StillHasUnprocessedFragments)
                            {
                                JobMetaHeap.Return(currJob);
                            }
                            else if( currJob.ProcessState != IoProduceble<TJob>.State.Syncing)
                            {
                                currJob.MoveUnprocessedToFragment();
                            }
                            currJob.ProcessState = IoProduceble<TJob>.State.Reject;
                        }

                        //Signal the producer that it can continue to get more work
                        try
                        {
                            PrimaryProducer.ProducerBarrier.Release(1);
                        }
                        catch
                        {
                            // ignored
                        }

                        if ((currJob.Id % parm_stats_mod_count) == 0)
                        {
                            _logger.Trace(
                                $"`{PrimaryProducerDescription}' consumer job heap = [[{JobMetaHeap.CacheSize()}/{JobMetaHeap.FreeCapacity()}/{JobMetaHeap.MaxSize}]]");
                            
                            currJob.ProducerHandle.PrintCounters();
                        }

                        //TODO remove this spam when everything checks out?
                        
                    }
                }
                else
                {
                    _logger.Warn($"`{PrimaryProducerDescription}' produced nothing");
                    PrimaryProducer.ProducerBarrier.Release(1);
                }
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (TimeoutException) { }
            catch (ArgumentNullException e) {
                _logger.Trace(e, $"Consumer `{PrimaryProducerDescription}' dequeue returned with errors:");
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Consumer `{PrimaryProducerDescription}' dequeue returned with errors:");
            }
            finally
            {
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
            using (cancellationToken.Register(() => Spinners.Cancel()))
            {
                //Producer
                var producerTask = Task.Factory.StartNew(async () =>
                {
                    //While not cancellation requested
                    while (!Spinners.IsCancellationRequested && spawnProducer)
                    {
                        await ProduceAsync(cancellationToken);
                        if (!PrimaryProducer.IsOperational)
                            break;
                    }
                }, Spinners.Token, TaskCreationOptions.LongRunning,  JobThreadScheduler);

                //Consumer
                var consumerTask = Task.Factory.StartNew(async () =>
                {
                    //While supposed to be working
                    while (!Spinners.IsCancellationRequested)
                    {
                        await ConsumeAsync();
                        if (!PrimaryProducer.IsOperational)
                            break;
                    }
                }, Spinners.Token, TaskCreationOptions.LongRunning, JobThreadScheduler);


                //consumerTask.Start();
                //producerTask.Start();

                //Wait for tear down                
                await Task.WhenAll(producerTask.Unwrap(), consumerTask.Unwrap()).ContinueWith(t=>
                {
                    PrimaryProducer.Close();
                }, cancellationToken);                                
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
