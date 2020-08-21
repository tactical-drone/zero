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
    /// Source Consumer pattern
    /// </summary>    
    /// <typeparam name="TJob">The type of job</typeparam>
    //public abstract class IoZero<TJob> : IoConfigurable, IObservable<IoLoad<TJob>>, IIoZero
    public abstract class IoZero<TJob> : IoConfigurable, IIoZero
        where TJob : IIoJob

    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A description of the progress</param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocMessage">A callback to malloc individual consumer jobs from the heap</param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IoLoad<TJob>> mallocMessage)
        {
            ConfigureProducer(description, source, mallocMessage);

            _logger = LogManager.GetCurrentClassLogger();

            //What to do when certain parameters change
            //SettingChangedEvent += (sender, pair) =>
            //{
            //    //update heap to match max Q size
            //    if (pair.Key == nameof(parm_max_q_size))
            //    {
            //        JobHeap.MaxSize = parm_max_q_size;
            //    }
            //};

            //prepare a multicast subject
            //ObservableRouter = this.Publish();
            //ObservableRouter.Connect();

            //Configure cancellations
            //Spinners.Token.Register(() => ObservableRouter.Connect().Dispose());         

            parm_stats_mod_count += new Random((int) DateTime.Now.Ticks).Next((int) (parm_stats_mod_count/2), parm_stats_mod_count);
        }

        /// <summary>
        /// Configures the source.
        /// </summary>
        /// <param name="description">A description of the source</param>
        /// <param name="source">An instance of the source</param>
        /// <param name="mallocMessage"></param>
        public void ConfigureProducer(string description, IoSource<TJob> source, Func<object, IoLoad<TJob>> mallocMessage)
        {
            Description = description;            
            Source = source;
            JobHeap = new IoHeapIo<IoLoad<TJob>>(parm_max_q_size) { Make = mallocMessage };

            Source.ClosedEvent((sender, args) => Close());
        }

        /// <summary>
        /// The source of the work
        /// </summary>
        public IoSource<TJob> Source;

        /// <summary>
        /// Upstream <see cref="Source"/> reference
        /// </summary>
        public IIoSource IoSource => Source; 

        /// <summary>
        /// The job queue
        /// </summary>
        private readonly ConcurrentQueue<IoLoad<TJob>> _queue = new ConcurrentQueue<IoLoad<TJob>>();

        /// <summary>
        /// A separate ingress point to the q
        /// </summary>
        private readonly BlockingCollection<IoLoad<TJob>> _ingressBalancer = new BlockingCollection<IoLoad<TJob>>();

        /// <summary>
        /// Q signaling
        /// </summary>
        private readonly AutoResetEvent _preInsertEvent = new AutoResetEvent(false);

        /// <summary>
        /// The heap where new consumable meta data is allocated from
        /// </summary>
        public IoHeapIo<IoLoad<TJob>> JobHeap { get; protected set; }

        /// <summary>
        /// Upstream reference to <see cref="JobHeap"/>
        /// </summary>
        public object IoJobHeap => JobHeap;

        /// <summary>
        /// A description of this source consumer
        /// </summary>
        public virtual string Description { get; protected set; }

        /// <summary>
        /// logger
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// Stops this source consumer
        /// </summary>
        protected CancellationTokenSource Spinners { get; set; } = new CancellationTokenSource();

        /// <summary>
        /// The current observer, there can only be one
        /// </summary>
        //private IObserver<IoLoad<TJob>> _observer;

        /// <summary>
        /// A connectable observer, used to multicast messages
        /// </summary>
        //public IConnectableObservable<IoLoad<TJob>> ObservableRouter { private set; get; }

        /// <summary>
        /// Indicates whether jobs are being processed
        /// </summary>
        public bool IsArbitrating { get; set; } = false;

        /// <summary>
        /// Maintains a handle to a job if fragmentation was detected so that the
        /// source can marshal fragments into the next production
        /// </summary>
        private volatile ConcurrentDictionary<long, IoLoad<TJob>>  _previousJobFragment = new ConcurrentDictionary<long, IoLoad<TJob>>();

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
        /// The amount of time to wait between retries when the source cannot allocate job management structures
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_error_timeout = 10000;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_consumer_threads = 2;

        /// <summary>
        /// The time that the source will delay waiting for the consumer to free up job buffer space
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_consumer_throttle_delay = 100;

        /// <summary>
        /// The time a source will wait for a consumer to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_consumer_wait_for_producer_timeout = 5000;

        /// <summary>
        /// How long a source will sleep for when it is getting skipped productions
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_start_retry_time = 1000;

        /// <summary>
        /// Called when this neighbor is closed
        /// </summary>
        // ReSharper disable once InconsistentNaming
        protected event EventHandler __closedEvent;

        /// <summary>
        /// Keeps tabs on all subscribers to be freed on close
        /// </summary>
        private readonly ConcurrentBag<EventHandler> _closedEventHandlers = new ConcurrentBag<EventHandler>();

        /// <summary>
        /// Enables safe subscriptions to close events
        /// </summary>
        /// <param name="del"></param>
        public void ClosedEvent(EventHandler del)
        {
            _closedEventHandlers.Add(del);
            __closedEvent += del;
        }

        /// <summary>
        /// Closed
        /// </summary>
        protected volatile bool Closed = false;

        /// <summary>
        /// Emits the closed event
        /// </summary>
        protected virtual void OnClosedEvent()
        {
            __closedEvent?.Invoke(this, EventArgs.Empty);
            //free handles
            _closedEventHandlers.ToList().ForEach(del => __closedEvent -= del);
            _closedEventHandlers.Clear();
        }

        /// <summary>
        /// Close 
        /// </summary>
        /// <returns>True if closed, false if it was closed</returns>
        public virtual bool Close()
        {
            lock (this)
            {
                if (Closed) return false;
                Closed = true;
            }

            try
            {
                OnClosedEvent();

                JobHeap.Clear();

                if (IsArbitrating || !Source.IsOperational )
                {
                    _logger.Debug($"Closing {Source.Key}");
                    Source.Close();
                }
                
                Spinners.Cancel();

                _logger.Debug($"Closed {ToString()}: {Description}");
            }
            catch (Exception e)
            {
                _logger.Trace(e, "Close returned with errors");
                return true;
            }

            return true;
        }

        /// <summary>
        /// Produces the inline instead of in a spin loop
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <param name="sleepOnConsumerLag">if set to <c>true</c> [sleep on consumer lag].</param>
        /// <returns></returns>
        public async Task<bool> ProduceAsync(bool sleepOnConsumerLag = true) //TODO fix sleepOnConsumerLag variable name that has double meaning
        {
            try
            {
                //_logger.Trace($"{nameof(ProduceAsync)}: `{Description}' [ENTER]");
                //And the consumer is keeping up
                if (_queue.Count < parm_max_q_size)
                {
                    IoLoad<TJob> nextJob = null;
                    bool jobSafeReleased = false;
                    try
                    {
                        //Allocate a job from the heap
                        if ((nextJob = JobHeap.Take()) != null)
                        {
                            nextJob.Zero = this;
                            if (nextJob.Id == 0)
                                IsArbitrating = true;
                            while (nextJob.Source.BlockOnProduceAheadBarrier)
                            {
                                if (!await nextJob.Source.ProduceAheadBarrier.WaitAsync(-1, Spinners.Token))
                                {
                                    Interlocked.Increment(ref nextJob.Source.NextProducerId());
                                    return false;
                                }

                                var nextProducerId = Interlocked.Read(ref nextJob.Source.NextProducerId());
                                if (nextJob.Id == nextProducerId)
                                {
                                    Interlocked.Increment(ref nextJob.Source.NextProducerId());
                                    break;
                                }
                                _logger.Warn($"{nextJob.TraceDescription} Next id = `{nextJob.Id}' is not {nextProducerId}!!");
                                nextJob.Source.ProduceAheadBarrier.Release();
                            }

                            //Fetch a job from TProducer. Did we get one?
                            _previousJobFragment.TryRemove(nextJob.Id - 1,  out var prevJobFragment);
                            nextJob.Previous = prevJobFragment;
                            nextJob.ProcessState = IoJob<TJob>.State.Producing;
                            if (await nextJob.ProduceAsync() < IoJob<TJob>.State.Error)
                            {
                                IsArbitrating = true;
                                //TODO Double check this hack
                                //Basically to handle this weird double connection business on the TCP iri side
                                //if (nextJob.ProcessState == IoJob<TJob>.State.ProStarting)
                                //{
                                //    nextJob.ProcessState = IoJob<TJob>.State.Produced;

                                //    if (sleepOnConsumerLag && nextJob.Source.Synced)
                                //        await Task.Delay(parm_producer_start_retry_time, cancellationToken);

                                //    Source.ProducerBarrier.Release(1);
                                //    return true;
                                //}
                            
                                _previousJobFragment.TryAdd(nextJob.Id, nextJob);

                                try
                                {
                                    if (nextJob.Source.BlockOnProduceAheadBarrier)
                                        nextJob.Source.ProduceAheadBarrier.Release();                                
                                }
                                catch
                                {
                                    // ignored
                                }

                                //Enqueue the job for the consumer
                                nextJob.ProcessState = IoJob<TJob>.State.Queued;
                            
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
                                    Source.ConsumerBarrier.Release(1);
                                }
                                catch
                                {
                                    // ignored
                                }                            
                            }
                            else //produce job returned with errors
                            {
                                IsArbitrating = false;

                                if (nextJob.Source.BlockOnProduceAheadBarrier)
                                    nextJob.Source.ProduceAheadBarrier.Release();

                                Source.ProducerBarrier.Release(1);

                                jobSafeReleased = true;
                                if (nextJob.Previous != null)
                                    _previousJobFragment.TryAdd(nextJob.Previous.Id + 1, (IoLoad<TJob>) nextJob.Previous);

                                Free(nextJob);

                                if (nextJob.ProcessState == IoJob<TJob>.State.Cancelled ||
                                    nextJob.ProcessState == IoJob<TJob>.State.ProdCancel)
                                {
                                    _logger.Debug($"{nextJob.TraceDescription} Source `{Description}' is shutting down");
                                    Close();
                                    return false;
                                }
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
                            if (nextJob.ProcessState != IoJob<TJob>.State.Finished)
                                nextJob.ProcessState = IoJob<TJob>.State.Reject;

                            Free(nextJob);                        
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (sleepOnConsumerLag)
                    {
                        _logger.Warn($"Source for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
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
                //_logger.Trace($"{nameof(ProduceAsync)}: `{Description}' [EXIT]");
            }

            return true;
        }

        private void Free(IoLoad<TJob> curJob)
        {            
            if (curJob.Previous != null)
            {
                if (_queue.Contains(curJob.Previous))
                {
                    _logger.Fatal($"Cannot remove job id = `{curJob.Previous.Id}' that is still queued!");
                    return;
                }

                JobHeap.Return((IoLoad<TJob>)curJob.Previous);
            }
                
        }
       
        /// <summary>
        /// Consumes the inline instead of from a spin loop
        /// </summary>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="sleepOnProducerLag">if set to <c>true</c> [sleep on source lag].</param>
        /// <returns></returns>
        public async Task<Task> ConsumeAsync(Func<IoLoad<TJob>,Task> inlineCallback = null, bool sleepOnProducerLag = true)
        {
            try
            {
                if (Source == null)
                    return Task.CompletedTask;

                //_logger.Trace($"{nameof(ConsumeAsync)}: `{Description}' [ENTER]");

                if (Source.BlockOnConsumeAheadBarrier && !await Source.ConsumeAheadBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout,
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
                        _logger.Warn($"Consumer `{Description}' [[ReadAheadBarrier]] timed out waiting on `{JobHeap.Make(null).Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                    }

                    //Try again                    
                    return Task.CompletedTask;
                }

                //Waiting for a job to be produced. Did production fail?
                if (!await Source.ConsumerBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, Spinners.Token))
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
                        _logger.Warn($"Consumer `{Description}' [[ConsumerBarrier]] timed out waiting on `{JobHeap.Make(null).Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                        await Task.Delay(parm_consumer_wait_for_producer_timeout/4);
                    }
                        

                    //Try again
                    return Task.CompletedTask;
                }

                //A job was produced. Dequeue it and process
                if (_queue.TryDequeue(out var curJob))
                {
                    curJob.ProcessState = IoJob<TJob>.State.Dequeued;
                    curJob.ProcessState = IoJob<TJob>.State.Consuming;
                    try
                    {
                        //Consume the job
                        if (await curJob.ConsumeAsync() >= IoJob<TJob>.State.Error)
                        {
                            _logger.Trace($"{curJob.TraceDescription} consuming job: `{curJob.Description}' was unsuccessful, state = {curJob.ProcessState}");                            
                        }
                        else
                        {
                            if (curJob.ProcessState == IoJob<TJob>.State.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob);
                            }

                            //Notify observer
                            //_observer?.OnNext(curJob);
                        }                        
                    }
                    catch (ArgumentNullException e)
                    {
                        _logger.Trace(e.InnerException ?? e,
                            $"{curJob.TraceDescription} consuming job: `{curJob.Description}' returned with errors:");
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e.InnerException ?? e,
                            $"{curJob.TraceDescription} consuming job: `{curJob.Description}' returned with errors:");
                    }
                    finally
                    {
                        if (Source.BlockOnConsumeAheadBarrier)
                            Source.ConsumeAheadBarrier.Release();

                        //Consume success?
                        if (curJob.ProcessState < IoJob<TJob>.State.Error)
                        {                            
                            curJob.ProcessState = IoJob<TJob>.State.Accept;                                                                    
                        }
                        else //Consume failed?
                        {                            
                            curJob.ProcessState = IoJob<TJob>.State.Reject;
                        }

                        try
                        {
                            //Signal the source that it can continue to get more work                        
                            Source.ProducerBarrier.Release(1);
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
                                curJob.Source.PrintCounters();
                                _logger.Debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                            }

                            Free(curJob);
                        }                                                                        
                    }
                }
                else
                {
                    _logger.Warn($"`{Description}' produced nothing");

                    Source.ProducerBarrier.Release(1);

                    if (Source.BlockOnConsumeAheadBarrier)
                        Source.ConsumeAheadBarrier.Release(1);                    
                }
            }            
            catch (ObjectDisposedException) { }
            catch (TimeoutException) { }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.Error(e, $"Consumer `{Description}' dequeue returned with errors:");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Starts this source consumer
        /// </summary>
        /// <param name="spawnProducer">Sometimes we don't want to start the source, for example when we are forwarding to another source consumer queue</param>
        public virtual async Task SpawnProcessingAsync(bool spawnProducer = true)
        {
            _logger.Trace($"Starting processing for `{Source.Description}'");
            
            //Source
            var producerTask = Task.Factory.StartNew(async () =>
            {
                //While not cancellation requested
                while (!Spinners.IsCancellationRequested && spawnProducer)
                {                        
                    await ProduceAsync().ConfigureAwait(false);
                    if (!Source.IsOperational)
                    {
                        _logger.Trace($"Source `{Description}' went non operational!");
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
                    await ConsumeAsync().ConfigureAwait(false);
                    if (!Source.IsOperational)
                    {
                        _logger.Trace($"Consumer `{Description}' went non operational!");
                        break;
                    }                            
                }
            }, TaskCreationOptions.LongRunning);

            //Wait for tear down                
            await Task.WhenAll(producerTask.Unwrap(), consumerTask.Unwrap()).ContinueWith(t=>
            {
                Source.Close();
            }, Spinners.Token);

            _logger.Trace($"Processing for `{Source.Description}' stopped");
        }

        /// <summary>
        /// Subscribe to this source, one at a time.
        /// </summary>
        /// <param name="observer">The observer that has to be notified</param>
        /// <returns></returns>
        //public IDisposable Subscribe(IObserver<IoLoad<TJob>> observer)
        //{
        //    _observer = observer;   
        //    return IoAnonymousDisposable.Create(() => { _observer = null; });
        //}
    }
}
