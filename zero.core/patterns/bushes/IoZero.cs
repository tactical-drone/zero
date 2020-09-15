using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
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
            //ProducerCount = 1;
            //ConsumerCount = 1;
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

            Source = source;

            Source.ZeroOnCascade(this, sourceZeroCascade);
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
        public int parm_stats_mod_count = 10000;
#else
        public int parm_stats_mod_count = 30000;
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

        private readonly Stopwatch _producerStopwatch = new Stopwatch();

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
        protected override async Task ZeroManagedAsync()
        {

            _queue.ToList().ForEach(q => q.ZeroAsync(this).ConfigureAwait(false));
            _queue.Clear();

            if (IsArbitrating || !Source.IsOperational)
            {
                await Source.ZeroAsync(this).ConfigureAwait(false);
            }

            _previousJobFragment.ToList().ForEach(job => job.ZeroAsync(this).ConfigureAwait(false));

            _previousJobFragment.Clear();

            await base.ZeroManagedAsync().ConfigureAwait(false);

            _logger.Trace($"Closed {Description}");
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
                        if (!Zeroed() && (nextJob = await JobHeap.TakeAsync(parms: (load,closure) =>
                        {
                            load.IoZero = (IIoZero) closure;
                            return load;
                        }, this).ConfigureAwait(false)) != null) //TODO 
                        {

                            nextJob.State = IoJobMeta.JobState.Producing;

                            if (nextJob.Id == 0)
                                IsArbitrating = true;

                            while (nextJob.Source.BlockOnProduceAheadBarrier && !Zeroed())
                            {
                                if (blockOnConsumerCongestion)
                                {
                                    try
                                    {
                                        await nextJob.Source.ProduceAheadBarrier.WaitAsync(AsyncTasks.Token);
                                    }
                                    catch
                                    {
                                        Interlocked.Increment(ref nextJob.Source.NextProducerId());
                                        return false;
                                    }
                                }
                                else
                                {
                                    try
                                    {
                                        await nextJob.Source.ProduceAheadBarrier.WaitAsync(AsyncTasks.Token).ConfigureAwait(false);
                                    }
                                    catch 
                                    {
                                        Interlocked.Increment(ref nextJob.Source.NextProducerId());
                                        return false;
                                    }
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
                                    //        kv.Value.ZeroAsync(this);
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
                                if (await nextJob.ProduceAsync(async (job, closure) =>
                                {
                                    var _this = (IoZero<TJob>)closure;
                                    //The producer barrier
                                    _this._producerStopwatch.Restart();
                                    try
                                    {
                                        try
                                        {
                                            await job.Source.ProducerBarrier.WaitAsync(_this.Source.AsyncTasks.Token).ConfigureAwait(false);
                                        }
                                        catch 
                                        {
                                            _this._producerStopwatch.Stop();
                                            job.State = IoJobMeta.JobState.ProdCancel;
                                            return false;
                                        }
                                    }
                                    catch (NullReferenceException e) { _this._logger.Trace(e); }
                                    catch (TaskCanceledException e) { _this._logger.Trace(e); }
                                    catch (OperationCanceledException e) { _this._logger.Trace(e); }
                                    catch (ObjectDisposedException e) { _this._logger.Trace(e); }
                                    catch (Exception e)
                                    {
                                        _this._logger.Error(e, $"Producer barrier failed for {_this.Description}");
                                        job.State = IoJobMeta.JobState.ProduceErr;
                                        return false;
                                    }

                                    return true;
                                }, this).ConfigureAwait(false) == IoJobMeta.JobState.Produced && !Zeroed())
                                {
                                    IsArbitrating = true;

                                    //_previousJobFragment.TryAdd(nextJob.Id, nextJob);
                                    if(SupportsSync)
                                        _previousJobFragment.Enqueue(nextJob);

                                    try
                                    {
                                        if (nextJob.Source.BlockOnProduceAheadBarrier)
                                            nextJob.Source.ProduceAheadBarrier.Set();                                
                                    }
                                    catch
                                    {
                                        // ignored
                                    }

                                    //Enqueue the job for the consumer
                                    nextJob.State = IoJobMeta.JobState.Queued;
                            
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
                                        Source.ConsumerBarrier.Set();
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
                                        nextJob.State = IoJobMeta.JobState.Reject;
                                        nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
                                        return false;
                                    }

                                    //if (nextJob.State == IoJobMeta.JobState.Producing)
                                    //{
                                    //    _logger.Warn($"{GetType().Name} ({nextJob.GetType().Name}): State remained {IoJobMeta.JobState.Producing}");
                                    //    nextJob.State = IoJobMeta.JobState.Cancelled;
                                    //}

                                    var aheadBarrier = nextJob.Source?.ProduceAheadBarrier;
                                    var releaseBarrier = nextJob.Source?.BlockOnProduceAheadBarrier ?? false;
                                    var barrier = Source?.ProducerBarrier;

                                    if (nextJob.State == IoJobMeta.JobState.Cancelled ||
                                        nextJob.State == IoJobMeta.JobState.ProdCancel)
                                    {
                                        _logger.Trace($"{GetType().Name}: {nextJob.TraceDescription} Source {Description} is shutting down");

                                        //Free job
                                        nextJob.State = IoJobMeta.JobState.Reject;
                                        nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
                                        IsArbitrating = false;

                                        await ZeroAsync(this).ConfigureAwait(false);

                                        return false;
                                    }

                                    //Free job
                                    nextJob.State = IoJobMeta.JobState.Reject;
                                    nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);

                                    // Release the next production after error
                                    if (releaseBarrier)
                                        aheadBarrier?.Set();
                                    barrier?.Set();
                                }
                            }
                        }
                        else
                        {                        
                            
                            if (!Zeroed())
                            {
                                _logger.Warn($"{GetType().Name}: Production for: `{Description}` failed. Cannot allocate job resources!");
                                await Task.Delay(parm_error_timeout, AsyncTasks.Token).ConfigureAwait(false);
                            }
                            return false;
                        }
                    }
                    catch (NullReferenceException) { nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false); }
                    catch (ObjectDisposedException) { nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false); }
                    catch (TimeoutException) { nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false); }
                    catch (TaskCanceledException) { nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false); }
                    catch (OperationCanceledException) { nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false); }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"{GetType().Name}: Producing `{Description}' returned with errors:");
                        nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
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
                            if (nextJob.State != IoJobMeta.JobState.Finished)
                            {
                                //_logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job status should be {nameof(IoJobMeta.JobState.Finished)}");
                                nextJob.State = IoJobMeta.JobState.Reject;
                            }
                            
                            nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (blockOnConsumerCongestion)
                    {
                        _logger.Warn($"{GetType().Name}: Source for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                        await Task.Delay(parm_producer_consumer_throttle_delay, AsyncTasks.Token).ConfigureAwait(false);
                    }
                }                
            }

            catch(TaskCanceledException) { return false; }
            catch(OperationCanceledException){ return false; }
            catch(ObjectDisposedException){ return false; }
            catch(NullReferenceException){ return false; }
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

        private async Task<IoLoad<TJob>> FreeAsync(IoLoad<TJob> job, bool parent = false)
        {
            if (job == null)
                return null;

            if (SupportsSync && job.PreviousJob != null)
            {
#if DEBUG
                if (((IoLoad<TJob>)job.PreviousJob).State != IoJobMeta.JobState.Finished)
                {
                    _logger.Warn($"{GetType().Name}: PreviousJob fragment state = {((IoLoad<TJob>)job.PreviousJob).State}");
                }
#endif
                await JobHeap.ReturnAsync((IoLoad<TJob>)job.PreviousJob).ConfigureAwait(false);
                job.PreviousJob = null;
                return null;
            }
            //else if (!parent && job.CurrentState.Previous.JobState == IoJobMeta.JobState.Accept)
            //{
            //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
            //}

            if(parent || !SupportsSync)
                await JobHeap.ReturnAsync(job).ConfigureAwait(false);

            return null;

            //try
            //{
            //    if (job.PreviousJob != null)
            //    {
            //        if (((IoLoad<TJob>) job.PreviousJob).State != IoJobMeta.JobState.Accept ||
            //            ((IoLoad<TJob>) job.PreviousJob).State != IoJobMeta.JobState.Reject)
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
            //        //    if (job.State != IoJobMeta.JobState.Accept ||
            //        //        job.State != IoJobMeta.JobState.Reject)
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
            //    job?.ZeroAsync(this);
            //    return null;
            //}

            //return job;
        }

        private long _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
       
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

                if (blockOnProduction && Source.BlockOnConsumeAheadBarrier)
                    
                {
                    try
                    {
                        await Source.ConsumeAheadBarrier.WaitAsync(AsyncTasks.Token).ConfigureAwait(false);
                    }
                    catch 
                    {
                        //Was shutdown requested?
                        if (Zeroed())
                        {
                            _logger.Trace($"{GetType().Name}: Consumer `{Description}' is shutting down");
                            return false;
                        }

                        _logger.Trace(
                            $"{GetType().Name}: Consumer `{Description}' [[ReadAheadBarrier]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");

                        //Try again                    
                        return false;
                    }
                }

                //Waiting for a job to be produced. Did production fail?
                if (blockOnProduction)
                {
                    try
                    {
                        await Source.ConsumerBarrier.WaitAsync(AsyncTasks.Token).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        //Was shutdown requested?
                        if (Zeroed() || AsyncTasks.IsCancellationRequested)
                        {
                            _logger.Trace($"{GetType().Name}: Consumer `{Description}' is shutting down");
                            await Task.Delay(10000);//TODO 
                            return false;
                        }

                        //wait...
                        //_logger.Trace(
                        //$"{GetType().Name}: Consumer `{Description}' [[ConsumerBarrier]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                        await Task.Delay(parm_consumer_wait_for_producer_timeout / 4, AsyncTasks.Token)
                            .ConfigureAwait(false);

                        //Try again
                        return false;
                    }
                }

                IoLoad<TJob> curJob = null;
                bool hadOneJob = false;

                //A job was produced. Dequeue it and process
                while (!Zeroed() && _queue.TryDequeue(out curJob))
                {
                    hadOneJob = true;
                    //curJob.State = IoJobMeta.JobState.Dequeued;
                    curJob.State = IoJobMeta.JobState.Consuming;
                    try
                    {
                        //Consume the job
                        if (await curJob.ConsumeAsync().ConfigureAwait(false) == IoJobMeta.JobState.Consumed || curJob.State == IoJobMeta.JobState.ConInlined  &&
                            !Zeroed())
                        {
                            consumed = true;

                            if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob).ConfigureAwait(false);
                                curJob.State = IoJobMeta.JobState.Consumed;
                            }

                            //Notify observer
                            //_observer?.OnNext(job);
                        }
                        else if (!Zeroed() && !curJob.Zeroed() && !Source.Zeroed())
                        {
                            _logger.Error(
                                $"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' was unsuccessful, state = {curJob.State}");
                            curJob.State = IoJobMeta.JobState.Error;
                        }
                    }
                    catch (NullReferenceException)
                    {
                    }
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
                        curJob.State = curJob.State < IoJobMeta.JobState.Error
                            ? IoJobMeta.JobState.Accept
                            : IoJobMeta.JobState.Reject;

                        try
                        {
                            if (curJob.Id > 0 && curJob.Id % parm_stats_mod_count == 0 && JobHeap.IoFpsCounter.Fps() < 1000 &&
                                DateTimeOffset.UtcNow.ToUnixTimeSeconds() - _lastStat > TimeSpan.FromMinutes(10).TotalSeconds)
                            {
                                _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                _logger.Info(
                                    "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                _logger.Info(
                                    $"{Description} {JobHeap.IoFpsCounter.Fps():F} j/s, [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.ReferenceCount + JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                curJob.Source.PrintCounters();
                                //_logger.Info("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                            }

                            curJob = await FreeAsync(curJob).ConfigureAwait(false);

                            if (Source.BlockOnConsumeAheadBarrier)
                                Source.ConsumeAheadBarrier.Set();

                            //Signal the source that it can continue to get more work                        
                                Source.ProducerBarrier.Set();
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }

                //did we do some work?
                if (consumed)
                    return true;

                if (Zeroed())
                    return false;

                if (hadOneJob)
                {
                    if (Source.BlockOnConsumeAheadBarrier)
                        Source.ConsumeAheadBarrier.Set();

                    Source.ProducerBarrier.Set();

                    _logger.Warn($"{GetType().Name}: `{Description}' produced nothing");
                }
                
                return false;
            }
            catch (NullReferenceException) { }
            catch (ObjectDisposedException) { }
            catch (TimeoutException) { }
            catch (TaskCanceledException) { }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }
            
            return false;
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


            Task<Task> consumerTask = null;

            //Producer
            var producerTask = Task.Factory.StartNew(async () =>
            {
                //While supposed to be working
                var producers = new Task[ProducerCount];
                while (!Zeroed())
                {
                    try
                    {
                        // ReSharper disable once PossibleNullReferenceException
                        // ReSharper disable once AccessToModifiedClosure
                        if(consumerTask!.Unwrap().IsCompleted)
                            break;
                    }
                    catch { }

                    for (var i = 0; i < ProducerCount; i++)
                    {

                        try
                        {
                            producers[i] = ProduceAsync();
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e, $"Production failed {Description}");
                        }
                    }

                    if (!Source?.IsOperational ?? false)
                    {
                        _logger.Trace($"{GetType().Name}: Producer {Description} went non operational!");
                        break;
                    }

                    await Task.WhenAll(producers).ConfigureAwait(false);
                }
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness);

            //Consumer
            consumerTask = Task.Factory.StartNew(async () =>
            {
                var consumers = new Task<bool>[ConsumerCount];
                //While supposed to be working
                while (!Zeroed() && !producerTask.Unwrap().IsCompleted )
                {
                    for (int i = 0; i < ConsumerCount; i++)
                    {
                        try
                        {
                            consumers[i] = ConsumeAsync();
                        }
                        catch (Exception e)
                        {
                            _logger.Error(e,$"Consumption failed {Description}");
                        }

                        
                    }

                    if (!Source?.IsOperational ?? false)
                    {
                        _logger.Trace($"{GetType().Name}: Consumer {Description} went non operational!");
                        break;
                    }

                    await Task.WhenAll(consumers);
                    
                    foreach (var c in consumers)
                    {
                        if (!c.Result)
                        {
                            _logger.Debug($"Failed to consume at {Description}");
                            break;
                        }
                    }
                }
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach );

            //Wait for tear down                
            await Task.WhenAll(producerTask.Unwrap(), consumerTask.Unwrap()).ConfigureAwait(false);

            await ZeroAsync(this).ConfigureAwait(false);

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
