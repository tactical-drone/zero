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
        /// <param name="producers">Nr of concurrent producers</param>
        /// <param name="consumers">Nr of concurrent consumers</param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IoLoad<TJob>> mallocJob, bool sourceZeroCascade = false, int producers = 1, int consumers = 1)
        {
            ProducerCount = producers;
            ConsumerCount = consumers;
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
            Func<object, IoLoad<TJob>> mallocMessage, bool sourceZeroCascade = false)
        {
            _description = description;
            JobHeap = ZeroOnCascade(new IoHeapIo<IoLoad<TJob>>(parm_max_q_size) { Make = mallocMessage }, true);

            source.ZeroOnCascade(this, sourceZeroCascade);
            Source = source;
        }

        /// <summary>
        /// The source of the work
        /// </summary>
        public IoSource<TJob> Source;

        /// <summary>
        /// Number of concurrent producers
        /// </summary>
        public int ProducerCount { get; protected set; }

        /// <summary>
        /// Number of concurrent consumers
        /// </summary>
        public int ConsumerCount { get; protected set; }

        /// <summary>
        /// Whether this source supports fragmented datums
        /// </summary>
        //protected bool SupportsSync => ConsumerCount == 1;
        protected bool SupportsSync => false;

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
        //private volatile ConcurrentDictionary<long, IoLoad<TJob>>  _previousJobFragment = new ConcurrentDictionary<long, IoLoad<TJob>>();
        private volatile ConcurrentQueue<IoLoad<TJob>> _previousJobFragment = new ConcurrentQueue<IoLoad<TJob>>();

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
#if DEBUG
        public int parm_stats_mod_count = 1000;
#else
        public int parm_stats_mod_count = 100000;
#endif

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
            _previousJobFragment = null;
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
                Source.Zero(this);
            }

            _previousJobFragment.ToList().ForEach(job => job.Zero(this));
            _previousJobFragment.Clear();

            base.ZeroManaged();
        }

        /// <summary>
        /// Produces the inline instead of in a spin loop
        /// </summary>
        /// <param name="blockOnConsumerCongestion">if set to <c>true</c> block on consumer congestion</param>
        /// <returns></returns>
        public async Task<bool> ProduceAsync(bool blockOnConsumerCongestion = true) 
        {
            try
            {
                //_logger.Fatal($"{nameof(ProduceAsync)}: `{Description}' [ENTER]");
                //And the consumer is keeping up
                if (_queue.Count < parm_max_q_size)
                {
                    IoLoad<TJob> nextJob = null;
                    try
                    {
                        //Allocate a job from the heap
                        if (!Zeroed() && (nextJob = JobHeap.Take(parms: load =>
                        {
                            ((IoLoad<TJob>) load).IoZero = this;
                            return load;
                        })) != null) //TODO 
                        {

                            nextJob.State = IoJob<TJob>.JobState.Producing;

                            if (nextJob.Id == 0)
                                IsArbitrating = true;

                            while (nextJob.Source.BlockOnProduceAheadBarrier && !Zeroed())//TODO this while loop needs to go
                            {
                                if (blockOnConsumerCongestion && !await nextJob.Source.ProduceAheadBarrier.WaitAsync(-1, AsyncTasks.Token))
                                {
                                    Interlocked.Increment(ref nextJob.Source.NextProducerId());
                                    return false;
                                }
                                else if(!await nextJob.Source.ProduceAheadBarrier.WaitAsync(1))
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
                                _logger.Warn($"{GetType().Name}: {nextJob.TraceDescription} Next id = `{nextJob.Id}' is not {nextProducerId}!!");
                                //nextJob.Source.ProduceAheadBarrier.Release();
                            }

                            //if(!Zeroed())
                            {
//sanity check _previousJobFragment
                                if (_previousJobFragment.Count > 100)
                                {
                                    _logger.Fatal($"({GetType().Name}<{typeof(TJob).Name}>) {nameof(_previousJobFragment)} id = {nextJob.Id} has grown large, zeroing...");
                                
                                    //_previousJobFragment.ToList().ForEach(kv =>
                                    //{
                                    //    if (kv.Value.Id != nextJob.Id - 1)
                                    //    {
                                    //        _previousJobFragment.TryRemove(kv.Key, out _);
                                    //        kv.Value.Zero(this);
                                    //    }
                                    //});

                                }

                                //if (!_previousJobFragment.TryRemove(nextJob.Id - 1, out var prevJobFragment))
                                //{
                                //    _previousJobFragment.ToList().ForEach(j=>Free(j.Value));
                                //    _previousJobFragment.Clear();
                                //}

                                if (SupportsSync && _previousJobFragment.TryDequeue(out var prevJobFragment))
                                {
                                    //if( prevJobFragment.Id == nextJob.Id + 1)
                                        nextJob.PreviousJob = prevJobFragment;
                                    //_logger.Fatal($"{GetHashCode()}:{nextJob.GetHashCode()}: {nextJob.Id}");
                                }
                                    

                                //if(prevJobFragment?.Id + 1 == nextJob.Id)

                                //else
                                //Free(prevJobFragment, true);

                                //Fetch a job from TProducer. Did we get one?
                                if (await nextJob.ProduceAsync() == IoJob<TJob>.JobState.Produced && !Zeroed())
                                {
                                    IsArbitrating = true;

                                    //_previousJobFragment.TryAdd(nextJob.Id, nextJob);
                                    if(SupportsSync)
                                        _previousJobFragment.Enqueue(nextJob);

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
                                    nextJob.State = IoJob<TJob>.JobState.Queued;
                            
                                    //if (!_queue.Contains(nextJob))
                                    {
                                        _queue.Enqueue(nextJob);
                                        nextJob = null;
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

                                    return true;
                                }
                                else //produce job returned with errors
                                {
                                    if (Zeroed())
                                    {
                                        //Free job
                                        nextJob.State = IoJob<TJob>.JobState.Reject;
                                        nextJob = Free(nextJob, true);
                                        return false;
                                    }

                                    //if (nextJob.State == IoJob<TJob>.JobState.Producing)
                                    //{
                                    //    _logger.Warn($"{GetType().Name} ({nextJob.GetType().Name}): State remained {IoJob<TJob>.JobState.Producing}");
                                    //    nextJob.State = IoJob<TJob>.JobState.Cancelled;
                                    //}

                                    var aheadBarrier = nextJob.Source?.ProduceAheadBarrier;
                                    var releaseBarrier = nextJob.Source?.BlockOnProduceAheadBarrier ?? false;
                                    var barrier = Source?.ProducerBarrier;

                                    if (nextJob.State == IoJob<TJob>.JobState.Cancelled ||
                                        nextJob.State == IoJob<TJob>.JobState.ProdCancel)
                                    {
                                        _logger.Debug($"{GetType().Name}: {nextJob.TraceDescription} Source `{Description}' is shutting down");

                                        //Free job
                                        nextJob.State = IoJob<TJob>.JobState.Reject;
                                        nextJob = Free(nextJob, true);
                                        IsArbitrating = false;
#pragma warning disable 4014
                                        Zero(this);
#pragma warning restore 4014
                                        return false;
                                    }

                                    //Free job
                                    nextJob.State = IoJob<TJob>.JobState.Reject;
                                    nextJob = Free(nextJob, true);

                                    // Release the next production after error
                                    if (releaseBarrier)
                                        aheadBarrier?.Release();
                                    barrier?.Release();
                                }
                            }
                        }
                        else
                        {                        
                            _logger.Fatal($"{GetType().Name}: Production for: `{Description}` failed. Cannot allocate job resources!");
                            await Task.Delay(parm_error_timeout, AsyncTasks.Token);
                            return false;
                        }
                    }
                    catch (NullReferenceException) { nextJob = Free(nextJob, true); }
                    catch (ObjectDisposedException) { nextJob = Free(nextJob, true); }
                    catch (TimeoutException) { nextJob = Free(nextJob, true); }
                    catch (OperationCanceledException) { nextJob = Free(nextJob, true); }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"{GetType().Name}: Producing `{Description}' returned with errors:");
                        nextJob = Free(nextJob, true);
                        return false;
                    }
                    finally
                    {
                        //prevent leaks
                        if (nextJob != null)
                        {
                            if(!(Zeroed() || nextJob.Zeroed()))
                                _logger.Error($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed...");

                            //TODO Double check this hack
                            if (nextJob.State != IoJob<TJob>.JobState.Finished)
                            {
                                //_logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job status should be {nameof(IoJob<TJob>.JobState.Finished)}");
                                nextJob.State = IoJob<TJob>.JobState.Reject;
                            }
                            
                            nextJob = Free(nextJob, true);
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (blockOnConsumerCongestion)
                    {
                        _logger.Warn($"{GetType().Name}: Source for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                        await Task.Delay(parm_producer_consumer_throttle_delay, AsyncTasks.Token);
                    }
                }                
            }
            catch(OperationCanceledException){}
            catch(ObjectDisposedException){}
            catch(NullReferenceException){}
            catch(Exception e)
            {
                _logger.Fatal(e, $"{GetType().Name}: {Description}: ");
                return false;
            }
            finally
            {
                //_logger.Fatal($"{nameof(ProduceAsync)}: `{Description}' [EXIT]");
            }

            return false;
        }

        private IoLoad<TJob> Free(IoLoad<TJob> job, bool parent = false)
        {
            if (job == null)
                return null;

            if (SupportsSync && job.PreviousJob != null)
            {
#if DEBUG
                if (((IoLoad<TJob>)job.PreviousJob).State != IoJob<TJob>.JobState.Finished)
                {
                    _logger.Warn($"{GetType().Name}: PreviousJob fragment state = {((IoLoad<TJob>)job.PreviousJob).State}");
                }
#endif
                JobHeap.Return((IoLoad<TJob>)job.PreviousJob);
                job.PreviousJob = null;
                return null;
            }
            //else if (!parent && job.CurrentState.Previous.JobState == IoJob<TJob>.JobState.Accept)
            //{
            //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
            //}

            if(parent || !SupportsSync)
                JobHeap.Return(job);

            return null;

            //try
            //{
            //    if (job.PreviousJob != null)
            //    {
            //        if (((IoLoad<TJob>) job.PreviousJob).State != IoJob<TJob>.JobState.Accept ||
            //            ((IoLoad<TJob>) job.PreviousJob).State != IoJob<TJob>.JobState.Reject)
            //        {
            //            _logger.Warn($"{GetType().Name}: JobState = {((IoLoad<TJob>)job.PreviousJob).State}");
            //        }

            //        JobHeap.Return((IoLoad<TJob>) job.PreviousJob);
            //        job.PreviousJob = null;
            //        return null;

            //        //if (job.StillHasUnprocessedFragments)
            //        //{
            //        //    _previousJobFragment.Enqueue(job);
            //        //    job = null;
            //        //}
            //        //else
            //        //{
            //        //    if (job.State != IoJob<TJob>.JobState.Accept ||
            //        //        job.State != IoJob<TJob>.JobState.Reject)
            //        //    {
            //        //        _logger.Warn($"{GetType().Name}: JobState = {job.State}");
            //        //    }

            //        //    JobHeap.Return(job);
            //        //    job = null;
            //        //}
            //    }
            //    else
            //    {
            //        JobHeap.Return(job);
            //        job = null;
            //    }
            //}
            //catch (Exception e)
            //{
            //    _logger.Debug(e, $"Free returned with errors");
            //    job?.Zero(this);
            //    return null;
            //}

            //return job;
        }
       
        /// <summary>
        /// Consumes the inline instead of from a spin loop
        /// </summary>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="blockOnProduction">if set to <c>true</c> block when production is not ready</param>
        /// <returns>True if consumption happened</returns>
        public async Task<bool> ConsumeAsync(Func<IoLoad<TJob>,Task> inlineCallback = null, bool blockOnProduction = true)
        {
            var consumed = false;
            try
            {
                //_logger.Fatal($"{nameof(ConsumeAsync)}: `{Description}' [ENTER]");

                if (Volatile.Read(ref Source) == null) //TODO why does this happen so often?
                    return false;

                if (blockOnProduction && Source.BlockOnConsumeAheadBarrier &&
                    !await Source.ConsumeAheadBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout,
                        AsyncTasks.Token))
                {
                    //Was shutdown requested?
                    if (Zeroed())
                    {
                        _logger.Debug($"{GetType().Name}: Consumer `{Description}' is shutting down");
                        return false;
                    }

                    _logger.Trace(
                        $"{GetType().Name}: Consumer `{Description}' [[ReadAheadBarrier]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");

                    //Try again                    
                    return false;
                }

                //Waiting for a job to be produced. Did production fail?
                if (blockOnProduction &&
                    !await Source.ConsumerBarrier.WaitAsync(parm_consumer_wait_for_producer_timeout, AsyncTasks.Token))
                {
                    //Was shutdown requested?
                    if (Zeroed())
                    {
                        _logger.Debug($"{GetType().Name}: Consumer `{Description}' is shutting down");
                        return false;
                    }

                    //wait...
                    _logger.Trace(
                        $"{GetType().Name}: Consumer `{Description}' [[ConsumerBarrier]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                    await Task.Delay(parm_consumer_wait_for_producer_timeout / 4);

                    //Try again
                    return false;
                }
                else if (!blockOnProduction && !await Source.ConsumerBarrier.WaitAsync(1, AsyncTasks.Token))
                {
                    return false;
                }

                //A job was produced. Dequeue it and process
                if (!Zeroed() && _queue.TryDequeue(out var curJob))
                {
                    
                    //curJob.State = IoJob<TJob>.JobState.Dequeued;
                    curJob.State = IoJob<TJob>.JobState.Consuming;
                    try
                    {
                        //Consume the job
                        if (await curJob.ConsumeAsync() == IoJob<TJob>.JobState.Consumed && !Zeroed() || curJob.State == IoJob<TJob>.JobState.ConInlined)
                        {
                            consumed = true;

                            if (curJob.State == IoJob<TJob>.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob);
                                curJob.State = IoJob<TJob>.JobState.Consumed;
                            }

                            //Notify observer
                            //_observer?.OnNext(job);
                        }
                        else if(!Zeroed() && !curJob.Zeroed() && !Source.Zeroed())
                        {
                            _logger.Error($"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' was unsuccessful, state = {curJob.State}");
                            curJob.State = IoJob<TJob>.JobState.Error;
                        }
                    }
                    catch (NullReferenceException) { }
                    catch (ArgumentNullException e)
                    {
                        _logger.Trace(e.InnerException ?? e,
                            $"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' returned with errors:");
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e.InnerException ?? e,
                            $"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' returned with errors:");
                    }
                    finally
                    {
                        //Consume success?
                        curJob.State = curJob.State < IoJob<TJob>.JobState.Error
                            ? IoJob<TJob>.JobState.Accept
                            : IoJob<TJob>.JobState.Reject;

                        try
                        {
                            if (curJob.Id % parm_stats_mod_count == 0)
                            {
                                _logger.Debug(
                                    "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                _logger.Debug(
                                    $"`{Description}' {JobHeap.IoFpsCounter.Fps():F} j/s, consumer job heap = [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                curJob.Source.PrintCounters();
                                _logger.Debug(
                                    "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                            }
                        }
                        catch
                        {
                            // ignored
                        }

                        curJob = Free(curJob);

                        try
                        {
                            if (Source.BlockOnConsumeAheadBarrier)
                                Source.ConsumeAheadBarrier.Release();
                        }
                        catch { /*Ignored*/ }

                        //Signal the source that it can continue to get more work                        
                        try
                        {
                            Source.ProducerBarrier.Release();
                        }
                        catch { /*ignored*/ }
                    }
                }
                else
                {
                    _logger.Warn($"{GetType().Name}: `{Description}' produced nothing");

                    if (Source.BlockOnConsumeAheadBarrier)
                        Source.ConsumeAheadBarrier.Release();

                    Source.ProducerBarrier.Release();

                    return false;
                }
            }
            catch (NullReferenceException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (TimeoutException)
            {
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Consumer `{Description}' dequeue returned with errors:");
                return false;
            }

            return consumed;
        }

        /// <summary>
        /// Starts this source consumer
        /// </summary>
        /// <param name="spawnProducer">Sometimes we don't want to start the source, for example when we are forwarding to another source consumer queue</param>
        public virtual async Task SpawnProcessingAsync(bool spawnProducer = true)
        {
            _logger.Trace($"{GetType().Name}: Starting processing for `{Source.Description}'");

            ////Source
            //var producerTask = Task.Factory.StartNew(async () =>
            //{
            //    //While not cancellation requested
            //    while (!Zeroed() && spawnProducer)
            //    {
            //        //var produceTask = ProduceAsync().ContinueWith(async t =>
            //        //{
            //        //    if (t.Result)
            //        //    {
            //        //        var doubleBuffer = await ProduceAsync(false);
            //        //        if ( doubleBuffer )
            //        //            _logger.Debug($"({GetType().Namespace})>> {Thread.CurrentThread.ManagedThreadId}");
            //        //        return doubleBuffer;
            //        //    }
            //        //    return false;
            //        //});

            //        //var consumeTask = ConsumeAsync().ContinueWith(async t =>
            //        //{
            //        //    if (t.Result)
            //        //    {
            //        //        var doubleBuffer = await ConsumeAsync(blockOnProduction: false);
            //        //        if ( doubleBuffer )
            //        //            _logger.Debug($"({GetType().Namespace})<< {Thread.CurrentThread.ManagedThreadId}");
            //        //        return doubleBuffer;
            //        //    }
            //        //    return false;
            //        //});

            //        Task<bool> consumeTask;
            //        Task<bool> prevConsumeTask = null;
            //        Task<bool> produceTask;

            //        void Continue(Task<bool> r)
            //        {
            //            if (!produceTask.IsCompleted && r.Status == TaskStatus.RanToCompletion && !r.Result)
            //            {
            //                prevConsumeTask = consumeTask;
            //                consumeTask = ConsumeAsync();
            //                consumeTask.ContinueWith(Continue);

            //                if (prevConsumeTask == null)
            //                    prevConsumeTask = consumeTask;
            //            }
            //            else
            //            {
            //                _logger.Debug($"C: {r.Status}");
            //            }
            //        }


            //        produceTask = ProduceAsync();
            //        produceTask.ContinueWith(r=>_logger.Debug($"P: {r.Status}"));
            //        consumeTask = ConsumeAsync();
            //        consumeTask.ContinueWith(Continue);

            //        if (!Source?.IsOperational??false)
            //        {
            //            _logger.Trace($"Source `{Description}' went non operational!");
            //            break;
            //        }

            //        try
            //        {
            //            while (Task.WaitAny(produceTask, consumeTask) >= 0)
            //            {
            //                Thread.Sleep(100);
            //                if (prevConsumeTask != null && produceTask.Status == TaskStatus.RanToCompletion &&
            //                    prevConsumeTask.Status == TaskStatus.RanToCompletion)
            //                    break;
            //                _logger.Trace($"{GetType().Name}: Process retrying... P = {produceTask.Status}, C = {consumeTask.Status}");
            //            }
            //        }
            //        catch (Exception e)
            //        {
            //            _logger.Debug(e, $"{GetType().Name}: Processing failed");
            //        }
            //    }

            //    _logger.Debug($"{GetType().Name}: Processing exited for {Description}");
            //}, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);


            //Producer
            var producerTask = Task.Factory.StartNew(async () =>
            {
                //While supposed to be working
                var producers = new Task[ProducerCount];
                while (!Zeroed())
                {
                    for (int i = 0; i < ProducerCount; i++)
                    {
                        producers[i] = ProduceAsync();
                        if (!Source?.IsOperational ?? false)
                        {
                            _logger.Debug($"{GetType().Name}: Producer `{Description}' went non operational!");
                            return;
                        }
                    }

                    await Task.WhenAll(producers).ConfigureAwait(true);
                }
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //Consumer
            var consumerTask = Task.Factory.StartNew(async () =>
            {
                var consumers = new Task[ConsumerCount];
                //While supposed to be working
                while (!Zeroed())
                {
                    for (int i = 0; i < ConsumerCount; i++)
                    {
                        consumers[i] = ConsumeAsync();
                        if (!Source?.IsOperational ?? false)
                        {
                            _logger.Debug($"{GetType().Name}: Consumer `{Description}' went non operational!");
                            return;
                        }
                    }

                    await Task.WhenAll(consumers).ConfigureAwait(true);
                }
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //Wait for tear down                
            var wait = Task.WhenAll(producerTask.Unwrap(), consumerTask.Unwrap()).ContinueWith(t=>
            {
                _logger.Debug($"{GetType().Name} [{t.Status}]: {Description}");
                Zero(this);
            });

            //var wait = Task.WhenAll(producerTask.Unwrap()).ContinueWith(t =>


            await wait;

            _logger.Trace($"{GetType().Name}: Processing for `{Description}' stopped");
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
