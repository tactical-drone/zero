using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Rendering;
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
        /// <param name="mallocJob">A callback to malloc individual consumer jobs from the heap</param>
        /// <param name="sourceZeroCascade">If the source zeroes out, so does this <see cref="IoZero{TJob}"/> instance</param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IoLoad<TJob>> mallocJob, bool sourceZeroCascade = true)
        {
            ConfigureProducer(description, source, mallocJob, sourceZeroCascade);

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
            //AsyncTasks.Token.Register(() => ObservableRouter.Connect().Dispose());         

            parm_stats_mod_count += new Random((int) DateTime.Now.Ticks).Next((int) (parm_stats_mod_count/2), parm_stats_mod_count);
        }

        /// <summary>
        /// Configures the source.
        /// </summary>
        /// <param name="description">A description of the source</param>
        /// <param name="source">An instance of the source</param>
        /// <param name="mallocMessage"></param>
        /// <param name="sourceZeroCascade"></param>
        public void ConfigureProducer(string description, IoSource<TJob> source,
            Func<object, IoLoad<TJob>> mallocMessage, bool sourceZeroCascade)
        {
            _description = description;
            JobHeap = new IoHeapIo<IoLoad<TJob>>(parm_max_q_size) { Make = mallocMessage };
            JobHeap.ZeroOnCascade(this, true);

            Source = source;
            Source.ZeroOnCascade(this, sourceZeroCascade);
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
        private ConcurrentQueue<IoLoad<TJob>> _queue = new ConcurrentQueue<IoLoad<TJob>>();

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
        /// Description backing field
        /// </summary>
        private string _description;

        /// <summary>
        /// A description of this source consumer
        /// </summary>
        public override string Description => _description;

        /// <summary>
        /// logger
        /// </summary>
        private readonly ILogger _logger;

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
        public int parm_stats_mod_count = 100;

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
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            Source = null;
            JobHeap = null;
            _queue = null;
#endif

        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            _queue.ToList().ForEach(q=>q.Zero(this));
            _queue.Clear();
            
            if (IsArbitrating || !Source.IsOperational)
            {
                _logger.Debug($"Zeroing source {Source.Key} for {Description}");
                Source.Zero(this);
            }

            _previousJobFragment.ToList().ForEach(pair => pair.Value.Zero(this));

            base.ZeroManaged();
        }

        /// <summary>
        /// Produces the inline instead of in a spin loop
        /// </summary>
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
                        if ((nextJob = JobHeap.Take()) != null) //TODO 
                        {
                            nextJob.IoZero = this;
                            if (nextJob.Id == 0)
                                IsArbitrating = true;

                            while (nextJob.Source.BlockOnProduceAheadBarrier && !Zeroed())
                            {
                                if (!await nextJob.Source.ProduceAheadBarrier.WaitAsync(-1, AsyncTasks.Token))
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

                            //if(!Zeroed())
                            {
//sanity check _previousJobFragment
                                if (_previousJobFragment.Count > 100)
                                {
                                    _logger.Fatal($"{nameof(_previousJobFragment)} has grown large, zeroing...");
                                
                                    _previousJobFragment.ToList().ForEach(kv =>
                                    {
                                        if (kv.Value.Id != nextJob.Id - 1)
                                        {
                                            _previousJobFragment.TryRemove(kv.Key, out _);
                                            kv.Value.Zero(this);
                                        }
                                    });
                                }

                                //Fetch a job from TProducer. Did we get one?
                                _previousJobFragment.TryRemove(nextJob.Id - 1,  out var prevJobFragment);
                                nextJob.Previous = prevJobFragment;
                                nextJob.ProcessState = IoJob<TJob>.State.Producing;
                                if (await nextJob.ProduceAsync() < IoJob<TJob>.State.Error && nextJob.ProcessState != IoJob<TJob>.State.Producing)
                                {
                                    IsArbitrating = true;

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
                                        _queue.Enqueue(nextJob);
                                        jobSafeReleased = true;
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
                                    //IsArbitrating = false;

                                    if (nextJob.ProcessState == IoJob<TJob>.State.Producing)
                                        throw new ApplicationException($"ProcessState remained {IoJob<TJob>.State.Producing}");

                                    if (nextJob.Source?.BlockOnProduceAheadBarrier??false)
                                        nextJob.Source?.ProduceAheadBarrier.Release();

                                    Source?.ProducerBarrier.Release(1);

                                    if (nextJob.Previous?.StillHasUnprocessedFragments??false)
                                    {
                                        _previousJobFragment.TryAdd(nextJob.Id, (IoLoad<TJob>)nextJob.Previous);
                                    }
                                    else
                                    {
                                        JobHeap?.Return(nextJob);
                                        await nextJob.Zero(this);
                                    }
                                    jobSafeReleased = true;


                                    if (nextJob.ProcessState == IoJob<TJob>.State.Cancelled ||
                                        nextJob.ProcessState == IoJob<TJob>.State.ProdCancel)
                                    {
                                        _logger.Debug($"{nextJob.TraceDescription} Source `{Description}' is shutting down");
#pragma warning disable 4014
                                        Zero(this);
#pragma warning restore 4014

                                        return false;
                                    }
                                }
                            }
                        }
                        else
                        {                        
                            _logger.Fatal($"Production for: `{Description}` failed. Cannot allocate job resources!");
                            await Task.Delay(parm_error_timeout, AsyncTasks.Token);
                        }
                    }
                    catch (NullReferenceException) {}
                    catch (ObjectDisposedException) { }
                    catch (TimeoutException) { }
                    catch (OperationCanceledException) { }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Producing `{Description}' returned with errors:");
                    }
                    finally
                    {
                        //prevent leaks
                        if (nextJob != null && !jobSafeReleased)
                        {
                            _logger.Debug("Job resources were not freed...");
                            //TODO Double check this hack
                            if (nextJob.ProcessState != IoJob<TJob>.State.Finished)
                                nextJob.ProcessState = IoJob<TJob>.State.Reject;

                            Free(nextJob);

                            nextJob.Previous?.Zero(this);
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (sleepOnConsumerLag)
                    {
                        _logger.Warn($"Source for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                        await Task.Delay(parm_producer_consumer_throttle_delay, AsyncTasks.Token);
                    }
                }                
            }
            catch(OperationCanceledException){}
            catch(ObjectDisposedException){}
            catch(NullReferenceException){}
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

        private void Free(IoLoad<TJob> job)
        {            
            if (job.Previous != null)
            {
                if (_queue.Contains(job.Previous))
                {
                    _logger.Fatal($"Cannot remove job id = `{job.Previous.Id}' that is still queued!");
                    return;
                }

                JobHeap.Return((IoLoad<TJob>)job.Previous);
                job.Previous = null;
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
                //_logger.Trace($"{nameof(ConsumeAsync)}: `{Description}' [ENTER]");

                if (Source.BlockOnConsumeAheadBarrier && !await Source.ConsumeAheadBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, AsyncTasks.Token))
                {
                    //Was shutdown requested?
                    if (Zeroed())
                    {
                        _logger.Debug($"Consumer `{Description}' is shutting down");
                        return Task.CompletedTask;
                    }

                    //Production timed out
                    if (sleepOnProducerLag)
                    {
                        _logger.Warn($"Consumer `{Description}' [[ReadAheadBarrier]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                    }

                    //Try again                    
                    return Task.CompletedTask;
                }

                //Waiting for a job to be produced. Did production fail?
                if (!await Source.ConsumerBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, AsyncTasks.Token))
                {
                    //Was shutdown requested?
                    if (Zeroed())
                    {
                        _logger.Debug($"Consumer `{Description}' is shutting down");
                        return Task.CompletedTask;
                    }
                    
                    //Production timed out
                    if (sleepOnProducerLag)
                    {                        
                        _logger.Warn($"Consumer `{Description}' [[ConsumerBarrier]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
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
                            //_observer?.OnNext(job);
                        }                        
                    }
                    catch (NullReferenceException) {}
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
                        try
                        {
                            if (Source.BlockOnConsumeAheadBarrier)
                                Source.ConsumeAheadBarrier.Release();
                        }
                        catch
                        {
                            // ignored
                        }

                        //Consume success?
                        curJob.ProcessState = curJob.ProcessState < IoJob<TJob>.State.Error ? IoJob<TJob>.State.Accept : IoJob<TJob>.State.Reject;

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
                            try
                            {
                                if ((curJob.Id % parm_stats_mod_count == 0))
                                {
                                    _logger.Debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                    _logger.Debug($"`{Description}' {JobHeap.IoFpsCounter.Fps():F} j/s, consumer job heap = [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                    curJob.Source.PrintCounters();
                                    _logger.Debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                }
                            }
                            catch
                            {
                                // ignored
                            }

                            Free(curJob);
                            curJob = null;
                        }                                                                        
                    }

                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (curJob != null)
                    {
                        await curJob.Zero(this);
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
            catch (NullReferenceException) { }
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
            var producerTask = Task.Factory.StartNew(() =>
            {
                //While not cancellation requested
                while (!Zeroed() && spawnProducer)
                {                        
                    var produceTask = ProduceAsync();
                    var consumeTask = ConsumeAsync();
                    if (!Source?.IsOperational??false)
                    {
                        _logger.Trace($"Source `{Description}' went non operational!");
                        break;
                    }

                    Task.WaitAll(produceTask, consumeTask);
                }
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            ////Consumer
            //var consumerTask = Task.Factory.StartNew(async () =>
            //{
            //    //While supposed to be working
            //    while (!Zeroed())
            //    {                        
            //        await ConsumeAsync().ConfigureAwait(false);
            //        if (!Source?.IsOperational??false)
            //        {
            //            _logger.Trace($"Consumer `{Description}' went non operational!");
            //            break;
            //        }                            
            //    }
            //}, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //Wait for tear down                
            //var wait = Task.WhenAll(producerTask.Unwrap(), consumerTask.Unwrap()).ContinueWith(t=>
            var wait = Task.WhenAll(producerTask).ContinueWith(t =>
            {
                Zero(this);
            });

            await wait;

            _logger.Trace($"Processing for `{Description}' stopped");
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
