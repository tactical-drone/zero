using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Org.BouncyCastle.Ocsp;
using zero.core.conf;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.schedulers;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Producer Consumer pattern
    /// </summary>
    /// <typeparam name="TConsumer">The consumer wrapper</typeparam>
    /// <typeparam name="TSource">Where work is sourced (produced) from</typeparam>
    public abstract class IoProducerConsumer<TConsumer, TSource> : IoConfigurable, IObservable<IoConsumable<TSource>>
        where TSource : IoJobSource
        where TConsumer : IoConsumable<TSource> 
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description"></param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocMessage">A callback to malloc individual consumer jobs from the heap</param>
        protected IoProducerConsumer(string description, TSource source, Func<TConsumer> mallocMessage)
        {
            Description = description;
            WorkSource = source;

            JobHeap = new IoHeapIo<TConsumer>(parm_max_q_size) { Make = mallocMessage };

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

            // set a default scheduler
            JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads);
        }

        /// <summary>
        /// The source of the messages
        /// </summary>
        public TSource WorkSource;

        /// <summary>
        /// The job queue
        /// </summary>
        private readonly ConcurrentQueue<TConsumer> _queue = new ConcurrentQueue<TConsumer>();

        /// <summary>
        /// A seperate ingress point to the q
        /// </summary>
        private readonly BlockingCollection<TConsumer> _ingressBalancer = new BlockingCollection<TConsumer>();

        /// <summary>
        /// Q signalling
        /// </summary>
        private readonly AutoResetEvent _preInsertEvent = new AutoResetEvent(false);

        /// <summary>
        /// The heap where new consumers are allocated from
        /// </summary>
        protected readonly IoHeapIo<TConsumer> JobHeap;

        /// <summary>
        /// A description of this producer consumer
        /// </summary>
        protected string Description;

        /// <summary>
        /// logger
        /// </summary>
        private readonly NLog.ILogger _logger;

        /// <summary>
        /// Stops this producer consumer
        /// </summary>
        public readonly CancellationTokenSource Spinners;

        /// <summary>
        /// The current observer, there can only be one
        /// </summary>
        private IObserver<IoConsumable<TSource>> _observer;

        /// <summary>
        /// A connectable observer, used to multicast messages
        /// </summary>
        public IConnectableObservable<IoConsumable<TSource>> ObservableRouter { private set; get; }

        /// <summary>
        /// The scheduler used to do work on
        /// </summary>
        protected LimitedThreadScheduler JobThreadScheduler;

        private TConsumer _previousJobFragment = null;

        /// <summary>
        /// Maximum amount of producers that can be buffered before we stop pruction of new jobs
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected long parm_max_q_size = 1000;

        /// <summary>
        /// Time to wait for insert before complaining about it
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_ingress_timout = 500;

        /// <summary>
        /// Debug output rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected long parm_stats_mod_count = 10;

        /// <summary>
        /// Used to rate limit this queue, in ms. Set to -1 for max rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_min_frame_time = 10;

        /// <summary>
        /// The amount of time to wait between retries when the producer cannnot allocate job management structures
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_error_timeout = 10000;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_max_consumer_threads = 2;

        /// <summary>
        /// The time that the producer will delay waiting for the consumer to free up job buffer space
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_producer_consumer_throttle_delay = 100;

        /// <summary>
        /// The time a producer will wait for a consumer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_consumer_wait_for_producer_timeout = 5000;


        /// <summary>
        /// Starts this producer consumer
        /// </summary>
        /// <param name="cancellationToken">The kill signal</param>
        public async Task SpawnProcessingAsync(CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => Spinners.Cancel()))
            {
                //Producer
                var producerTask = Task.Factory.StartNew(async () =>
                {
                    //While not cancellation requested
                    while (!Spinners.IsCancellationRequested)
                    {
                        //And the consumer is keeping up
                        if (_queue.Count < parm_max_q_size)
                        {
                            TConsumer nextJob = null;
                            bool wasQueued = false;
                            try
                            {
                                //Allocate a job from the heap
                                if ((nextJob = JobHeap.Take()) != null)
                                {                                    
                                    //Fetch a job from TSource. Did we get one?
                                    if (await nextJob.ProduceAsync(_previousJobFragment) < IoProducable<TSource>.State.Error)
                                    {
                                        TConsumer currJobFragment = null;
                                        
                                        //Does production yield fragmented datums?
                                        if (nextJob.ProcessState == IoProducable<TSource>.State.Fragmented)
                                        {
                                            nextJob.ProcessState = IoProducable<TSource>.State.Produced;
                                            currJobFragment = nextJob;
                                        }
                                        
                                        //Enqueue the job for the consumer
                                        nextJob.ProcessState = IoProducable<TSource>.State.Queued;
                                        _queue.Enqueue(nextJob);
                                        wasQueued = true;

                                        //Signal to the consumer that there is work to do
                                        WorkSource.ConsumerBarrier.Release(1);

                                        //Accept a previous fragment
                                        //if (_previousJobFragment != null)
                                        //    JobHeap.Return(_previousJobFragment);                                            

                                        //Prepare this production's remaining datum fragments for the next production
                                        _previousJobFragment = currJobFragment;
                                    }
                                    else //produce job returned with errors
                                    {
                                        if (nextJob.ProcessState == IoProducable<TSource>.State.Cancelled)
                                        {
                                            Spinners.Cancel();
                                            _logger.Debug($"Producer `{Description}' is shutting down");
                                            break;
                                        }
                                        
                                        //TODO double check what we are supposed to do here?
                                        //TODO what about the previous fragment?
                                        var sleepTimeMs = nextJob.Source.Counters[(int)IoProducable<TSource>.State.Reject] + 1 / (nextJob.Source.Counters[(int)IoProducable<TSource>.State.Accept] + parm_error_timeout);
                                        _logger.Debug($"{nextJob.Description}, Reject = {nextJob.Source.Counters[(int)IoProducable<TSource>.State.Reject]}, Accept = {nextJob.Source.Counters[(int)IoProducable<TSource>.State.Accept]}");
                                        _logger.Debug($"`{Description}' producing job `{nextJob.Description}' returned with state `{nextJob.ProcessState}', sleeping for {sleepTimeMs}ms...");

                                        await Task.Delay((int)sleepTimeMs, Spinners.Token);
                                    }
                                }
                                else
                                {
                                    //TODO will this ever happen?
                                    _logger.Warn($"Producing for `{Description}` failed. Cannot allocate job resources");
                                    await Task.Delay(parm_error_timeout, Spinners.Token);
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"Producing `{Description}' returned with errors:");
                            }
                            finally
                            {
                                //prevent leaks
                                if (nextJob != null && !wasQueued)
                                {
                                    nextJob.ProcessState = IoProducable<TSource>.State.Reject;
                                    JobHeap.Return(nextJob);
                                }
                            }
                        }
                        else //We have run out of buffer space. Wait for the consumer to catch up
                        {
                            _logger.Warn($"Producer for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                            await Task.Delay(parm_producer_consumer_throttle_delay, Spinners.Token);
                        }
                    }
                }, TaskCreationOptions.LongRunning);

                //Consumer
                var consumerTask = Task.Factory.StartNew(async () =>
                {
                    //While supposed to be working
                    while (!Spinners.IsCancellationRequested)
                    {
                        try
                        {
                            //Waiting for a job to be produced. Did production fail?
                            if (!await WorkSource.ConsumerBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, Spinners.Token))
                            {
                                //Was shutdown requested?
                                if (Spinners.IsCancellationRequested)
                                {
                                    _logger.Debug($"Consumer `{Description}' is shutting down");
                                    break;
                                }

                                //Production timed out
                                _logger.Warn($"Consumer `{Description}' timed out waiting for producer, willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");

                                //Try again
                                continue;                                
                            }

                            //A job was produced. Dequeue it and process
                            if (_queue.TryDequeue(out var currJob))
                            {
                                try
                                {
                                    //Consume the job
                                    if (await currJob.ConsumeAsync() >= IoProducable<TSource>.State.Error)
                                    {
                                        _logger.Warn($"`{Description}' consuming job `{currJob.Description}' was unsuccessful, state = {currJob.ProcessState}");
                                    }

                                    //Notify observer
                                    _observer?.OnNext(currJob);
                                }
                                catch (Exception e)
                                {
                                    _logger.Error(e.InnerException ?? e, $"`{Description}' consuming job `{currJob.Description}' returned with errors:");
                                }
                                finally
                                {
                                    //Signal the producer that it continue to get more work
                                    WorkSource.ProducerBarrier.Release(1);

                                    //Consume success?
                                    if (currJob.ProcessState == IoProducable<TSource>.State.Consumed)
                                    {
                                        currJob.ProcessState = IoProducable<TSource>.State.Accept;

                                        JobHeap.Return(currJob);
                                    } 
                                    else //Consume failed?
                                    {
                                        currJob.ProcessState = IoProducable<TSource>.State.Reject;
                                        JobHeap.Return(currJob);
                                    }

                                    if ((currJob.Id % parm_stats_mod_count) == 0)
                                    {
                                        _logger.Trace($"`{Description}' consumer job heap = [[{JobHeap.CacheSize()}/{JobHeap.FreeCapacity()}/{JobHeap.MaxSize}]]");
                                        //job.PrintStateHistory();                                            
                                    }

                                    //TODO remove this spam when everything checks out?
                                    currJob.Source.PrintCounters();
                                }
                            }
                            else
                            {
                                _logger.Warn($"`{Description}' producer signalled that a job is ready but nothing found in the jobQueue. Strange BUG!");
                                WorkSource.ProducerBarrier.Release(1);
                            }
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, "Consumer dequeuer returned with errors:");
                        }
                        finally
                        {
                            //GC.Collect(GC.MaxGeneration);
                        }
                    }
                }, TaskCreationOptions.LongRunning);

                //Wait for teardown

                await Task.WhenAll(new Task[] { producerTask, consumerTask });
            }

        }


        //        /// <summary>
        //        /// Starts this producer consumer
        //        /// </summary>
        //        /// <param name="cancellationToken">The kill signal</param>
        //        public async Task SpawnProcessingAsyncold(CancellationToken cancellationToken)
        //        {
        //            using (cancellationToken.Register(() => Spinners.Cancel()))
        //            {
        //                //main loop
        //                _logger.Debug($"Starting producer/consumer `{Description}'");

        //                while (!Spinners.IsCancellationRequested)
        //                {
        //                    //CONSUME
        //                    if (_queue.Count > 0)
        //                    {
        //                        //-----------------------------------------------------------------
        //                        // ASYNC WORK
        //                        //-----------------------------------------------------------------
        //#pragma warning disable 4014
        //                        Task.Factory.StartNew(() =>
        //#pragma warning restore 4014
        //                        {
        //                            try
        //                            {
        //                                while (_queue.TryTake(out var currJob, 0, Spinners.Token))
        //                                {
        //                                    try
        //                                    {
        //                                        try
        //                                        {
        //                                            //TODO async this function instead of the Task.Run
        //                                            //call the work callback                                    
        //                                            if (ConsumeAsync(currJob, _previousJobFragment) >= IoProducable<TSource>.State.Error)
        //                                            {
        //                                                _logger.Warn($"Consuming job `{currJob.Description}' returned with errors, state = {currJob.ProcessState}");
        //                                            }

        //                                            //Notify observer
        //                                            _observer?.OnNext(currJob);
        //                                        }
        //                                        catch (Exception e)
        //                                        {
        //                                            _logger.Error(e.InnerException ?? e,
        //                                                $"Consuming job `{currJob.Description}' returned with errors:");
        //                                        }

        //                                        if ((currJob.Id % parm_stats_mod_count) == 0)
        //                                        {
        //                                            _logger.Trace($"`{Description}' consumer job heap = [[{JobHeap.CacheSize()}/{JobHeap.FreeCapacity()}/{JobHeap.MaxSize}]]");
        //                                            //job.PrintStateHistory();                                            
        //                                        }
        //                                    }
        //                                    finally
        //                                    {
        //                                        if (currJob != null)
        //                                        {
        //                                            //Consume success?
        //                                            if (currJob.ProcessState == IoProducable<TSource>.State.Consumed)
        //                                            {
        //                                                currJob.ProcessState = IoProducable<TSource>.State.Accept;

        //                                                JobHeap.Return(currJob);

        //                                                //Also accept the previous job fragment
        //                                                if (_previousJobFragment != null)
        //                                                {
        //                                                    _previousJobFragment.ProcessState = IoProducable<TSource>.State.Consumed;
        //                                                    _previousJobFragment.ProcessState = IoProducable<TSource>.State.Accept;
        //                                                    JobHeap.Return(_previousJobFragment);
        //                                                    _previousJobFragment = null;
        //                                                }
        //                                            } //Consume signalled a job with remaining datum fragment
        //                                            else if (currJob.ProcessState == IoProducable<TSource>.State.Fragmented)
        //                                            {
        //                                                // Finalize the previous job fragment
        //                                                if (_previousJobFragment != null)
        //                                                {
        //                                                    _previousJobFragment.ProcessState = IoProducable<TSource>.State.Consumed;
        //                                                    _previousJobFragment.ProcessState = IoProducable<TSource>.State.Accept;

        //                                                    JobHeap.Return(_previousJobFragment);
        //                                                }

        //                                                // Prepare a job fragment for the next consume from the current 
        //                                                _previousJobFragment = currJob;
        //                                            }
        //                                            else //Consume failed?
        //                                            {
        //                                                currJob.ProcessState = IoProducable<TSource>.State.Reject;
        //                                                JobHeap.Return(currJob);
        //                                                if (_previousJobFragment != null)
        //                                                {
        //                                                    _previousJobFragment.ProcessState = IoProducable<TSource>.State.Reject;
        //                                                    JobHeap.Return(_previousJobFragment);
        //                                                    _previousJobFragment = null;
        //                                                }
        //                                            }

        //                                            currJob.Source.PrintCounters();
        //                                        }
        //                                    }
        //                                }
        //                            }
        //                            catch (Exception e)
        //                            {
        //                                _logger.Error(e, "Consumer dequeuer returned with errors:");
        //                            }
        //                            finally
        //                            {
        //                                //GC.Collect(GC.MaxGeneration);
        //                            }

        //                        }, Spinners.Token, TaskCreationOptions.LongRunning, JobThreadScheduler);
        //                    }

        //                    //Rate limit producer
        //                    //TODO fix
        //                    if (parm_min_frame_time > -1)
        //                    {
        //                        //Thread.Sleep((int)parm_min_frame_time);
        //                        await Task.Delay(parm_min_frame_time, Spinners.Token);
        //                    }

        //                    //PRODUCE
        //                    if (_queue.Count < parm_max_q_size)
        //                    {
        //                        TConsumer nextJob = null;
        //                        try
        //                        {
        //                            //Allocate a job from the heap                                                
        //                            nextJob = JobHeap.Take();

        //                            if (nextJob != null)
        //                            {
        //                                //Find new jobs                        
        //                                nextJob.ProcessState = IoProducable<TSource>.State.Producing;
        //                                if (await nextJob.ProduceAsync() < IoProducable<TSource>.State.Error)
        //                                {
        //                                    try
        //                                    {
        //                                        nextJob.ProcessState = IoProducable<TSource>.State.Queued;
        //                                        _queue.Add(nextJob, Spinners.Token);
        //                                    }
        //                                    catch (Exception e)
        //                                    {
        //                                        _logger.Error(e,
        //                                            $"Adding job to queue failed, IsCancellationRequested = `{Spinners.Token.IsCancellationRequested}'");
        //                                        nextJob.ProcessState = IoProducable<TSource>.State.ProduceCancelled;
        //                                    }
        //                                }
        //                                else
        //                                {
        //                                    if (nextJob.ProcessState == IoProducable<TSource>.State.Cancelled)
        //                                        Spinners.Cancel();

        //                                    var sleepTimeMs =
        //                                        nextJob.Source.Counters[(int)IoProducable<TSource>.State.Reject] + 1 /
        //                                        (nextJob.Source.Counters[(int)IoProducable<TSource>.State.Accept] +
        //                                         parm_error_timeout);
        //                                    _logger.Debug(
        //                                        $"{nextJob.Description}, Reject = {nextJob.Source.Counters[(int)IoProducable<TSource>.State.Reject]}, Accept = {nextJob.Source.Counters[(int)IoProducable<TSource>.State.Accept]}");
        //                                    _logger.Debug(
        //                                        $"Producing job `{nextJob.Description}' did not complete, sleeping for {sleepTimeMs}ms...");
        //                                    Thread.Sleep((int)sleepTimeMs);

        //                                    continue;
        //                                }


        //                                //TODO Switch priorities?
        //                                //Try find some jobs from the side
        //                                if (nextJob.ProcessState != IoProducable<TSource>.State.Queued &&
        //                                    _ingressBalancer.TryTake(out var job))
        //                                {
        //                                    //Que for work
        //                                    job.ProcessState = IoProducable<TSource>.State.Produced;
        //                                    try
        //                                    {
        //                                        job.ProcessState = IoProducable<TSource>.State.Queued;
        //                                        _queue.Add(job, Spinners.Token);
        //                                        //boiler
        //                                    }
        //                                    catch (Exception e)
        //                                    {
        //                                        _logger.Error(e,
        //                                            $"Adding job to queue failed, IsCancellationRequested = `{Spinners.Token.IsCancellationRequested}'");
        //                                        job.ProcessState = IoProducable<TSource>.State.ProduceCancelled;
        //                                    }
        //                                }
        //                            }
        //                            else
        //                            {
        //                                _logger.Trace($"Producing `{Description}` failed. Cannot allocate job structure");
        //                                Thread.Sleep((int)parm_error_timeout);
        //                            }
        //                        }
        //                        catch (OperationCanceledException)
        //                        {
        //                            _logger.Debug($"Producer `{Description}' was stopped");
        //                        }
        //                        catch (Exception e)
        //                        {
        //                            _logger.Error(e, $"Producing `{Description}' returned with errors:");
        //                        }
        //                        finally
        //                        {
        //                            //prevent leaks
        //                            if (nextJob != null && nextJob.ProcessState != IoProducable<TSource>.State.Queued)
        //                            {
        //                                nextJob.ProcessState = IoProducable<TSource>.State.Reject;
        //                                JobHeap.Return(nextJob);
        //                            }
        //                        }
        //                    }
        //                    else //We have run out of buffer space. Go consume
        //                    {
        //                        _logger.Trace(
        //                            $"Producer consumer `{Description}' waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
        //                    }
        //                }
        //            }
        //        }

        /// <summary>
        /// Subscribe to this producer, one at a time.
        /// </summary>
        /// <param name="observer">The observer that has to be notified</param>
        /// <returns></returns>
        public IDisposable Subscribe(IObserver<IoConsumable<TSource>> observer)
        {
            _observer = observer;
            return IoAnonymousDiposable.Create(() => { _observer = null; });
        }
    }
}
