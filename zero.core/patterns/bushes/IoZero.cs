﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes
{
    /// <summary>
    /// Source Consumer pattern
    /// </summary>    
    /// <typeparam name="TJob">The type of job</typeparam>
    //public abstract class IoZero<TJob> : IoNanoprobe, IObservable<IoLoad<TJob>>, IIoZero
    public abstract class IoZero<TJob> : IoNanoprobe, IIoZero
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
            
            //JobHeap = ZeroOnCascade(new IoHeapIo<IoLoad<TJob>>(parm_max_q_size) { Make = mallocMessage }, true);
            JobHeap = new IoHeapIo<IoLoad<TJob>>(parm_max_q_size) { Make = mallocMessage };

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
        public bool IsArbitrating { get; set; }

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
        public long parm_max_q_size = 100;

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
        public int parm_producer_consumer_throttle_delay = 1000;
        
        /// <summary>
        /// Minimum time a failed production should block
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_failed_production_time = 100;

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
        /// Used to detect spamming non performing producers
        /// </summary>
        private readonly Stopwatch _producerStopwatch = new Stopwatch();

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
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
        public override async ValueTask ZeroManagedAsync()
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
        /// Produce
        /// </summary>
        /// <param name="enablePrefetchOption">if set to <c>true</c> block on consumer congestion</param>
        /// <returns></returns>
        public async ValueTask<bool> ProduceAsync(bool enablePrefetchOption = true) 
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
                            return new ValueTask<IoLoad<TJob>>(load);
                        }, this).ConfigureAwait(false)) != null) //TODO 
                        {

                            nextJob.State = IoJobMeta.JobState.Producing;

                            if (nextJob.Id == 0)
                                IsArbitrating = true;
                            
                            //wait for prefetch pressure
                            if (nextJob.Source.PrefetchEnabled && enablePrefetchOption)
                            {
                                var prefetch = nextJob.Source.WaitForPrefetchPressureAsync();
                                await prefetch.OverBoostAsync().ConfigureAwait(false);
                                if (!prefetch.Result)
                                {
                                    return false;
                                }
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
                                
                                if (SupportsSync && _previousJobFragment.TryDequeue(out var prevJobFragment))
                                {
                                    //if( prevJobFragment.Id == nextJob.Id + 1)
                                        nextJob.PreviousJob = prevJobFragment;
                                    //_logger.Fatal($"{GetHashCode()}:{nextJob.GetHashCode()}: {nextJob.Id}");
                                }
                                
                                _producerStopwatch.Restart();
                                //Produce job input
                                if (await nextJob.ProduceAsync(async (job, closure) =>
                                {
                                    var _this = (IoZero<TJob>)closure;
                                    //Block on producer backpressure
                                    try
                                    {
                                        var backPressure = job.Source.WaitForBackPressureAsync();
                                        await backPressure.OverBoostAsync();

                                        if (!backPressure.Result)
                                        {
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
                                    _producerStopwatch.Stop();
                                    IsArbitrating = true;

                                    //if(SupportsSync)
                                        //_previousJobFragment.TryAdd(nextJob.Id, nextJob);
                                    
                                    if(SupportsSync)
                                        _previousJobFragment.Enqueue(nextJob);
                                    
                                    //signal back pressure
                                    if (nextJob.Source.PrefetchEnabled  && enablePrefetchOption)
                                        nextJob.Source.PrefetchPressure();                                
                                    
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

                                    Source.Pressure();
                                    
                                    return true;
                                }
                                else //produce job returned with errors or nothing...
                                {
                                    //how long did this failure take?
                                    _producerStopwatch.Stop();
                                    
                                    //Are we in teardown?
                                    if (Zeroed())
                                    {
                                        //Free job
                                        nextJob.State = IoJobMeta.JobState.Reject;
                                        nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
                                        return false;
                                    }
                                    
#if  DEBUG
                                    //Is there a bug in the producer?
                                    if (nextJob.State == IoJobMeta.JobState.Producing)
                                    {
                                        _logger.Warn($"{GetType().Name} ({nextJob.GetType().Name}): State remained {IoJobMeta.JobState.Producing}");
                                        nextJob.State = IoJobMeta.JobState.Error;
                                    }
#endif
                                    
                                    //Handle failure that leads to teardown
                                    if (nextJob.State == IoJobMeta.JobState.Cancelled ||
                                        nextJob.State == IoJobMeta.JobState.ProdCancel ||
                                        nextJob.State == IoJobMeta.JobState.Error)
                                    {
                                        _logger.Trace($"{nameof(ProduceAsync)}: [FAILED], s = `{nextJob}', {Description}");

                                        //Free job
                                        nextJob.State = IoJobMeta.JobState.Reject;
                                        nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
                                        IsArbitrating = false;

                                        //TEARDOWN
                                        await ZeroAsync(this).ConfigureAwait(false);
                                        return false;
                                    }
                                    
                                    //Is the producer spinning?
                                    if (_producerStopwatch.ElapsedMilliseconds < parm_min_failed_production_time)
                                        await Task.Delay(parm_min_failed_production_time, AsyncTokenProxy.Token).ConfigureAwait(false);

                                    //Free job
                                    nextJob.State = IoJobMeta.JobState.Reject;
                                    nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);

                                    // Signal prefetch back pressure
                                    if (nextJob.Source.PrefetchEnabled  && enablePrefetchOption)
                                        nextJob.Source.PrefetchPressure();
                                    
                                    //signal back pressure
                                    Source.BackPressure();
                                }
                            }
                        }
                        else
                        {
                            //Are we in teardown?
                            if (Zeroed()) return false;
                            
                            _logger.Warn($"{GetType().Name}: Production for: `{Description}` failed. Cannot allocate job resources!");
                            await Task.Delay(parm_error_timeout, AsyncTokenProxy.Token).ConfigureAwait(false);
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
                                //_logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job status should be {nameof(IoJobMeta.CurrentState.Finished)}");
                                nextJob.State = IoJobMeta.JobState.Reject;
                            }
                            
                            nextJob = await FreeAsync(nextJob, true).ConfigureAwait(false);
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (enablePrefetchOption)
                    {
                        _logger.Warn($"{GetType().Name}: Source for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                        await Task.Delay(parm_producer_consumer_throttle_delay, AsyncTokenProxy.Token).ConfigureAwait(false);
                    }
                }                
            }

            catch(TaskCanceledException e) { _logger.Trace(e,Description); }
            catch(OperationCanceledException e){ _logger.Trace(e,Description); }
            catch(ObjectDisposedException e){ _logger.Trace(e,Description); }
            catch(NullReferenceException e){ _logger.Trace(e,Description); }
            catch(Exception e)
            {
                _logger.Fatal(e, $"{GetType().Name}: {Description}: ");
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
            //else if (!parent && job.CurrentState.Previous.CurrentState == IoJobMeta.CurrentState.Accept)
            //{
            //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
            //}

            if(parent || !SupportsSync)
                await JobHeap.ReturnAsync(job).ConfigureAwait(false);

            return null;
        }

        private long _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        /// <summary>
        /// Consume 
        /// </summary>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="zeroClosure"></param>
        /// <param name="blockOnProduction">if set to <c>true</c> block when production is not ready</param>
        /// <returns>True if consumption happened</returns>
        public async ValueTask<bool> ConsumeAsync(Func<IoLoad<TJob>, IIoZero, Task> inlineCallback = null, IIoZero zeroClosure = null)
        {
            var consumed = 0;
            try
            {
                //_logger.Fatal($"{nameof(ConsumeAsync)}: `{Description}' [ENTER]");

                if (Volatile.Read(ref Source) == null) //TODO why does this happen so often?
                    return false;
                
                //Waiting for a job to be produced. Did production fail?
                //if (_queue.IsEmpty) //TODO autoresetevent
                {
                    //wait for the producer pressure
                    var pressure = Source.WaitForPressureAsync();
                    
                    //fast path
                    await pressure.OverBoostAsync().ConfigureAwait(false);
                    
                    //Wait for producer pressure
                    if (!pressure.Result)
                    {
                        //Was shutdown requested?
                        if (Zeroed() || AsyncTokenProxy.IsCancellationRequested)
                        {
                            _logger.Trace($"{GetType().Name}: Consumer `{Description}' is shutting down");
                            return false;
                        }

                        //wait...
                        //_logger.Trace(
                        //$"{GetType().Name}: Consumer `{Description}' [[ProducerPressure]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                        await Task.Delay(parm_consumer_wait_for_producer_timeout / 4, AsyncTokenProxy.Token).ConfigureAwait(false);

                        //Try again
                        return false;    
                    }
                }

                IoLoad<TJob> curJob = null;
                var hadOneJob = 0;

                //A job was produced. Dequeue it and process
                while (!Zeroed() && _queue.TryDequeue(out curJob)) //TODO autoresetevent
                {
                    hadOneJob++;
                    //curJob.State = IoJobMeta.CurrentState.Dequeued;
                    curJob.State = IoJobMeta.JobState.Consuming;
                    try
                    {
                        //Consume the job
                        if (await curJob.ConsumeAsync().ConfigureAwait(false) == IoJobMeta.JobState.Consumed || curJob.State == IoJobMeta.JobState.ConInlined  &&
                            !Zeroed())
                        {
                            consumed++;

                            if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob, zeroClosure).ConfigureAwait(false);
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
                    catch (NullReferenceException e)
                    {
                        _logger.Trace(e, Description);
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
                            
                            if (curJob.Id > 0 && curJob.Id % parm_stats_mod_count == 0 && JobHeap.IoFpsCounter.Fps() < 10000000 &&
                                _lastStat.UtDelta() > TimeSpan.FromSeconds(10).TotalSeconds)
                            {
                                lock (Environment.Version)
                                {
                                    _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                    _logger.Info(
                                        "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                    _logger.Info(
                                        $"{Description} {JobHeap.IoFpsCounter.Fps():F} j/s, [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.ReferenceCount + JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                    curJob.Source.PrintCounters();
                                    //_logger.Info("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                }
                            }
                                
                            curJob = await FreeAsync(curJob).ConfigureAwait(false);

                            //if (Source.BlockOnConsumeAheadBarrier)
                            //    Source.ConsumeAheadBarrier.Set();

                            //Signal the source that it can continue to get more work                        
                            //if(consumed)
                                //Source.BackPressure();
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }

                //did we do some work?
                if (consumed > 0)
                {
                    Source.BackPressure(consumed);
                    return true;
                }

                //were we zeroed?
                if (Zeroed())
                    return false;

                //did we get a job but source queue was empty when we got there?
                if (hadOneJob > 0)
                {
                    //if (Source.BlockOnConsumeAheadBarrier)
                    //    Source.ConsumeAheadBarrier.Set();

                    //Source.ProduceBackPressure.Set();

                    _logger.Warn($"{GetType().Name}: `{Description}' produced nothing");
                }
                
                return false;
            }
            catch (NullReferenceException e) { _logger.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TimeoutException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }
            finally
            {
                //if (Source != null)
                //{
                //    if (Source.BlockOnConsumeAheadBarrier)
                //        Source.ConsumeAheadBarrier.Set();

                //    //Signal the source that it can continue to get more work
                //    Source.ProduceBackPressure.Set();
                //}
            }

            return false;
        }

        /// <summary>
        /// Starts the processors
        /// </summary>
        public virtual async Task AssimilateAsync()
        {
            _logger.Debug($"{GetType().Name}: Assimulating {Description}");
            
            Task<Task> consumerTask = null;

            //Producer
            var producerTask = Task.Factory.StartNew(async () =>
            {
                //While supposed to be working
                var producers = new ValueTask<bool>[ProducerCount];
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
                            //fast path
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
                    
                    //wait for all to completes
                    for (int i = 0; i < producers.Length; i++)
                    {
                        //slow path
                        if (!producers[i].IsCompletedSuccessfully)
                            await producers[i].ConfigureAwait(false);
                    }
                    
                }
            }, AsyncTokenProxy.Token,TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach , TaskScheduler.Default);

            //Consumer
            consumerTask = Task.Factory.StartNew(async () =>
            {
                var consumers = new ValueTask<bool>[ConsumerCount];
                
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

                    //wait for all to completes
                    for (int i = 0; i < consumers.Length; i++)
                    {
                        //slow path
                        if (!consumers[i].IsCompletedSuccessfully)
                            await consumers[i].ConfigureAwait(false);
                    }
                }
            }, AsyncTokenProxy.Token, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

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
