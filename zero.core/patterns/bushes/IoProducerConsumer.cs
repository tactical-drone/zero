using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using NLog;
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
        where TConsumer : IoConsumable<TSource> where TSource : IoConcurrentProcess
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description"></param>
        /// <param name="mallocMessage">A callback to malloc individual consumer jobs from the heap</param>
        protected IoProducerConsumer(string description, Func<TConsumer> mallocMessage)
        {
            Description = description;

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
        /// The job queue
        /// </summary>
        private readonly BlockingCollection<TConsumer> _queue = new BlockingCollection<TConsumer>();

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

        private TConsumer _nextJobRemainingFragment = null;

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
        /// The amount of time to wait between retries when the producer returns exceptions
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected long parm_error_timeout = 1000;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_max_consumer_threads = 2;

        /// <summary>
        /// Reentrant producer
        /// </summary>
        /// <exception cref="InternalBufferOverflowException">Thrown from <see cref="IoHeap{T}"/> when the max heap size is breached</exception>
        /// <param name="job">The job to be queued</param>
        /// <returns>true if the job was queued</returns>
        public bool Produced(TConsumer job)
        {
            try
            {
                //lightweight prebuffer
                job.ProcessState = IoProducable<TSource>.State.Producing;
                while (!_ingressBalancer.TryAdd(job, parm_ingress_timout, Spinners.Token))
                {
                    //hot lock detection
                    job.ProcessState = IoProducable<TSource>.State.ProduceTo;
                    _logger.Warn($"Pre-insert job `{job.Description}' queuing for {(DateTime.Now - job.CurrentState.Previous.ExitTime).TotalMilliseconds:F}ms...");
                }

                job.Source.ProduceSemaphore.Release(1);
            }
            catch (Exception e)
            {
                job.ProcessState = IoProducable<TSource>.State.ProduceErr;
                _logger.Error(e, $"Pre-insert returned with errors");
                return false;
            }

            job.ProcessState = IoProducable<TSource>.State.Produced;
            return true;
        }

        /// <summary>
        /// Consumer callback
        /// </summary>
        /// <param name="currJob">The current job fragment to be procesed</param>
        /// <param name="currJobPreviousFragment">Include a previous job fragment if job spans two productions</param>        
        /// <returns>The state of the consumer</returns>
        protected abstract IoProducable<TSource>.State Consume(TConsumer currJob, TConsumer currJobPreviousFragment = null);

        /// <summary>
        /// Starts this producer consumer
        /// </summary>
        /// <param name="cancellationToken">The kill signal</param>
        public async Task SpawnProcessingAsync(CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => Spinners.Cancel()))
            {
                //main loop
                _logger.Debug($"Starting producer/consumer `{Description}'");

                while (!Spinners.IsCancellationRequested)
                {
                    //CONSUME
                    if (_queue.Count > 0)
                    {
                        //-----------------------------------------------------------------
                        // ASYNC WORK
                        //-----------------------------------------------------------------
                        #pragma warning disable 4014
                        Task.Factory.StartNew(() =>
                        #pragma warning restore 4014
                        {
                            try
                            {
                                while (_queue.TryTake(out var currJob, 0, Spinners.Token))
                                {
                                    try
                                    {
                                        currJob.ProcessState = IoProducable<TSource>.State.Consuming;
                                        try
                                        {
                                            //TODO async this function instead of the Task.Run
                                            //call the work callback                                    
                                            if (Consume(currJob, _nextJobRemainingFragment) >= IoProducable<TSource>.State.Error)
                                            {
                                                _logger.Warn($"Consuming job `{currJob.Description}' did not complete");
                                            }

                                            //Notify observer
                                            _observer?.OnNext(currJob);
                                        }
                                        catch (Exception e)
                                        {
                                            _logger.Error(e.InnerException ?? e,
                                                $"Consuming job `{currJob.Description}' returned with errors:");
                                        }

                                        //if ((job.Id % parm_stats_mod_count) == 0)
                                        {
                                            _logger.Trace($"`{Description}' consumer job heap = [[{JobHeap.CacheSize()}/{JobHeap.FreeCapacity()}/{JobHeap.MaxSize}]]");
                                            //job.PrintStateHistory();                                            
                                        }
                                    }
                                    finally
                                    {
                                        if (currJob != null)
                                        {                                            
                                            //Consume success?
                                            if (currJob.ProcessState == IoProducable<TSource>.State.Consumed)
                                            {
                                                currJob.ProcessState = IoProducable<TSource>.State.Accept;

                                                JobHeap.Return(currJob);

                                                //Also accept the previous job fragment
                                                if (_nextJobRemainingFragment != null)
                                                {
                                                    _nextJobRemainingFragment.ProcessState = IoProducable<TSource>.State.Accept;
                                                    JobHeap.Return(_nextJobRemainingFragment);
                                                    _nextJobRemainingFragment = null;
                                                }                                                    
                                            } //Consume signalled a job with remaining datum fragment
                                            else if (currJob.ProcessState == IoProducable<TSource>.State.ConsumerFragmented)
                                            {
                                                // Finalize the previous job fragment
                                                if (_nextJobRemainingFragment != null)
                                                {
                                                    _nextJobRemainingFragment.ProcessState = IoProducable<TSource>.State.Consumed;
                                                    _nextJobRemainingFragment.ProcessState = IoProducable<TSource>.State.Accept;

                                                    JobHeap.Return(_nextJobRemainingFragment);
                                                }

                                                // Prepare a job fragment for the next consume from the current 
                                                _nextJobRemainingFragment = currJob;
                                            }                                                    
                                            else //Consume failed?
                                            {
                                                currJob.ProcessState = IoProducable<TSource>.State.Reject;
                                                JobHeap.Return(currJob);
                                                if (_nextJobRemainingFragment != null)
                                                {
                                                    _nextJobRemainingFragment.ProcessState = IoProducable<TSource>.State.Reject;
                                                    JobHeap.Return(_nextJobRemainingFragment);
                                                    _nextJobRemainingFragment = null;
                                                }
                                            }
                                            
                                            currJob.Source.PrintCounters();
                                        }
                                    }
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

                        }, Spinners.Token, TaskCreationOptions.LongRunning, JobThreadScheduler);
                    }

                    //Rate limit producer
                    //TODO fix
                    if (parm_min_frame_time > -1)
                    {
                        //Thread.Sleep((int)parm_min_frame_time);
                        await Task.Delay(parm_min_frame_time, Spinners.Token);
                    }

                    //PRODUCE
                    if (_queue.Count < parm_max_q_size)
                    {
                        TConsumer nextJob = null;
                        try
                        {
                            //Allocate a job from the heap                                                
                            nextJob = JobHeap.Take();

                            if (nextJob != null)
                            {
                                //Find new jobs                        
                                nextJob.ProcessState = IoProducable<TSource>.State.Producing;
                                if (await nextJob.ProduceAsync() < IoProducable<TSource>.State.Error)
                                {
                                    try
                                    {
                                        nextJob.ProcessState = IoProducable<TSource>.State.Queued;
                                        _queue.Add(nextJob, Spinners.Token);
                                    }
                                    catch (Exception e)
                                    {
                                        _logger.Error(e,
                                            $"Adding job to queue failed, IsCancellationRequested = `{Spinners.Token.IsCancellationRequested}'");
                                        nextJob.ProcessState = IoProducable<TSource>.State.ProduceCancelled;
                                    }
                                }
                                else
                                {
                                    if (nextJob.ProcessState == IoProducable<TSource>.State.Cancelled)
                                        Spinners.Cancel();

                                    var sleepTimeMs =
                                        nextJob.Source.Counters[(int) IoProducable<TSource>.State.Reject] + 1 /
                                        (nextJob.Source.Counters[(int) IoProducable<TSource>.State.Accept] +
                                         parm_error_timeout);
                                    _logger.Debug(
                                        $"{nextJob.Description}, Reject = {nextJob.Source.Counters[(int) IoProducable<TSource>.State.Reject]}, Accept = {nextJob.Source.Counters[(int) IoProducable<TSource>.State.Accept]}");
                                    _logger.Debug(
                                        $"Producing job `{nextJob.Description}' did not complete, sleeping for {sleepTimeMs}ms...");
                                    Thread.Sleep((int) sleepTimeMs);

                                    continue;
                                }


                                //TODO Switch priorities?
                                //Try find some jobs from the side
                                if (nextJob.ProcessState != IoProducable<TSource>.State.Queued &&
                                    _ingressBalancer.TryTake(out var job))
                                {
                                    //Que for work
                                    job.ProcessState = IoProducable<TSource>.State.Produced;
                                    try
                                    {
                                        job.ProcessState = IoProducable<TSource>.State.Queued;
                                        _queue.Add(job, Spinners.Token);
                                        //boiler
                                    }
                                    catch (Exception e)
                                    {
                                        _logger.Error(e,
                                            $"Adding job to queue failed, IsCancellationRequested = `{Spinners.Token.IsCancellationRequested}'");
                                        job.ProcessState = IoProducable<TSource>.State.ProduceCancelled;
                                    }
                                }
                            }
                            else
                            {
                                _logger.Trace($"Producing `{Description}` failed. Cannot allocate job structure");
                                Thread.Sleep((int) parm_error_timeout);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            _logger.Debug($"Producer `{Description}' was stopped");
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, $"Producing `{Description}' returned with errors:");
                        }
                        finally
                        {
                            //prevent leaks
                            if (nextJob != null && nextJob.ProcessState != IoProducable<TSource>.State.Queued)
                            {
                                nextJob.ProcessState = IoProducable<TSource>.State.Reject;
                                JobHeap.Return(nextJob);
                            }
                        }
                    }
                    else //We have run out of buffer space. Go consume
                    {
                        _logger.Trace(
                            $"Producer consumer `{Description}' waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                    }
                }
            }
        }

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
