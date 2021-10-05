using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

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
        /// <param name="enableSync"></param>
        /// <param name="sourceZeroCascade">If the source zeroes out, so does this <see cref="IoZero{TJob}"/> instance</param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IoSink<TJob>> mallocJob,
            bool enableSync, bool sourceZeroCascade = false, int concurrencyLevel = -1) : base($"{nameof(IoSink<TJob>)}", concurrencyLevel < 0? source.ZeroConcurrencyLevel() : concurrencyLevel)
        {
            

            ConfigureProducer(description, source, mallocJob, sourceZeroCascade);

            _logger = LogManager.GetCurrentClassLogger();

            SyncRecoveryModeEnabled = enableSync;

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

            if (SyncRecoveryModeEnabled)
                _previousJobFragment = new IoZeroQueue<IoSink<TJob>>($"{Description}", 5, ZeroConcurrencyLevel());
        }

        /// <summary>
        /// Configures the source.
        /// </summary>
        /// <param name="description">A description of the source</param>
        /// <param name="source">An instance of the source</param>
        /// <param name="mallocMessage"></param>
        /// <param name="sourceZeroCascade"></param>
        public void ConfigureProducer(string description, IoSource<TJob> source,
            Func<object, IoSink<TJob>> mallocMessage, bool sourceZeroCascade = false)
        {
            _description = description;
            
            JobHeap = new IoHeapIo<IoSink<TJob>>(parm_max_q_size) { Make = mallocMessage };

            Source = source;

            _queue = new IoZeroQueue<IoSink<TJob>>($"zero Q: {_description}", ZeroConcurrencyLevel(), ZeroConcurrencyLevel());

            ZeroOnCascade(Source, sourceZeroCascade);
        }

        /// <summary>
        /// The source of the work
        /// </summary>
        public IoSource<TJob> Source;

        ///// <summary>
        ///// Number of concurrent producers
        ///// </summary>
        //public int ProducerCount { get; protected set; }

        ///// <summary>
        ///// Number of concurrent consumers
        ///// </summary>
        //public int ConsumerCount { get; protected set; }

        /// <summary>
        /// Whether this source supports fragmented datums
        /// </summary>
        //protected bool SyncRecoveryModeEnabled => ConsumerCount == 1;
        public bool SyncRecoveryModeEnabled { get; protected set; }

        /// <summary>
        /// Upstream <see cref="Source"/> reference
        /// </summary>
        public IIoSource IoSource => Source;

        /// <summary>
        /// The job queue
        /// </summary>
        private IoZeroQueue<IoSink<TJob>> _queue;
        
        /// <summary>
        /// The heap where new consumable meta data is allocated from
        /// </summary>
        public IoHeapIo<IoSink<TJob>> JobHeap { get; protected set; }

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
        private ILogger _logger;

        /// <summary>
        /// Indicates whether jobs are being processed
        /// </summary>
        public bool IsArbitrating { get; set; }

        /// <summary>
        /// Maintains a handle to a job if fragmentation was detected so that the
        /// source can marshal fragments into the next production
        /// </summary>
        private volatile IoZeroQueue<IoSink<TJob>> _previousJobFragment;

        /// <summary>
        /// Maximum amount of producers that can be buffered before we stop production of new jobs
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_max_q_size = 500; //TODO

        /// <summary>
        /// Minimum useful uptime in seconds
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_uptime = 2;

        /// <summary>
        /// Debug output rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
#if DEBUG
        public int parm_stats_mod_count = 500000;
#else
        public int parm_stats_mod_count = 50000;
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
        
        /// <summary>
        /// The amount of time to wait between retries when the source cannot allocate job management structures
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_conduit_spin_up_wait_time = 0;

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
            _logger = null;
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
            await _queue.ZeroManagedAsync(static async (sink,@this) => await sink.ZeroAsync(@this).FastPath().ConfigureAwait(false), this).FastPath().ConfigureAwait(false);

            if(_previousJobFragment != null)
                await _previousJobFragment.ZeroManagedAsync(static async (job, @this) => await job.ZeroAsync(@this).FastPath().ConfigureAwait(false),this).FastPath().ConfigureAwait(false);

            await JobHeap.ZeroManagedAsync(static async (sink, @this) =>
            {
                await sink.ZeroAsync(@this).FastPath().ConfigureAwait(false);
            },this).FastPath().ConfigureAwait(false);

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);

            _logger.Trace($"Closed {Description}, from :{ZeroedFrom?.Description}");
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
                    IoSink<TJob> nextJob = null;
                    try
                    {
                        nextJob = await JobHeap.TakeAsync(parms: (load, closure) =>
                        {
                            load.IoZero = (IIoZero)closure;
                            return new ValueTask<IoSink<TJob>>(load);
                        }, this).FastPath().ConfigureAwait(false);

                        //Allocate a job from the heap
                        if (!Zeroed() && nextJob != null)
                        {

                            nextJob.State = IoJobMeta.JobState.Producing;

                            if (nextJob.Id == 0)
                                IsArbitrating = true;

                            //wait for prefetch pressure
                            if (nextJob.Source.PrefetchEnabled && enablePrefetchOption)
                            {
                                if (!await nextJob.Source.WaitForPrefetchPressureAsync().FastPath()
                                    .ConfigureAwait(false))
                                {
                                    return false;
                                }
                            }

                            if (!Zeroed())
                            {
//sanity check _previousJobFragment
                                if (SyncRecoveryModeEnabled && _previousJobFragment.Count >= JobHeap.MaxSize *2/3)
                                {
                                    _logger.Warn(
                                        $"({GetType().Name}<{typeof(TJob).Name}>) {nameof(_previousJobFragment)} id = {nextJob.Id} has grown large, zeroing...");

                                    //_previousJobFragment.ToList().ForEach(kv =>
                                    //{
                                    //    if (kv.Value.Id != nextJob.Id - 1)
                                    //    {
                                    //        _previousJobFragment.TryRemove(kv.Key, out _);
                                    //        kv.Value.ZeroAsync(this);
                                    //    }
                                    //});

                                }


                                IoSink<TJob> prevJobFragment;
                                if (SyncRecoveryModeEnabled &&
                                    (prevJobFragment = await _previousJobFragment.DequeueAsync().FastPath()
                                        .ConfigureAwait(false)) != null)
                                {
                                    //if( prevJobFragment.Id == nextJob.Id + 1)
                                    nextJob.PreviousJob = prevJobFragment;
                                    //_logger.Fatal($"{GetHashCode()}:{nextJob.GetHashCode()}: {nextJob.Id}");
                                }

                                _producerStopwatch.Restart();

                                //prepare a failed job to sync with next job
                                if (SyncRecoveryModeEnabled)
                                    nextJob.JobSync();

                                //Produce job input
                                if (await nextJob.ProduceAsync(static async (job, @this) =>
                                {
                                    //Block on producer back pressure
                                    try
                                    {
                                        var backPressure = await job.Source.WaitForBackPressureAsync().FastPath()
                                            .ConfigureAwait(false);

                                        if (!backPressure)
                                        {
                                            job.State = IoJobMeta.JobState.ProdCancel;
                                            return false;
                                        }
                                    }
                                    catch (NullReferenceException e) when (!@this.Zeroed())
                                    {
                                        @this._logger.Trace(e);
                                    }
                                    catch (TaskCanceledException e) when (!@this.Zeroed())
                                    {
                                        @this._logger.Trace(e);
                                    }
                                    catch (OperationCanceledException e) when (!@this.Zeroed())
                                    {
                                        @this._logger.Trace(e);
                                    }
                                    catch (ObjectDisposedException e) when (!@this.Zeroed())
                                    {
                                        @this._logger.Trace(e);
                                    }
                                    catch (Exception) when(@this.Zeroed()){}
                                    catch (Exception e)
                                    {
                                        @this._logger.Error(e, $"Producer barrier failed for {@this.Description}");
                                        job.State = IoJobMeta.JobState.ProduceErr;
                                        return false;
                                    }

                                    return true;
                                }, this).FastPath().ConfigureAwait(false) == IoJobMeta.JobState.Produced && !Zeroed())
                                {
                                    _producerStopwatch.Stop();
                                    IsArbitrating = true;

                                    if (SyncRecoveryModeEnabled)
                                        await _previousJobFragment.EnqueueAsync(nextJob).FastPath()
                                            .ConfigureAwait(false);

                                    //signal back pressure
                                    if (nextJob.Source.PrefetchEnabled && enablePrefetchOption)
                                        nextJob.Source.PrefetchPressure();

                                    //Enqueue the job for the consumer
                                    nextJob.State = IoJobMeta.JobState.Queued;

                                    if (await _queue.EnqueueAsync(nextJob).FastPath().ConfigureAwait(false) == null)
                                        return false;

                                    nextJob = null;

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
                                        //FreeAsync job
                                        nextJob.State = IoJobMeta.JobState.Reject;
                                        nextJob = await FreeAsync(nextJob, true).FastPath().ConfigureAwait(false);
                                        return false;
                                    }

#if DEBUG
                                    //Is there a bug in the producer?
                                    if (nextJob.State == IoJobMeta.JobState.Producing)
                                    {
                                        _logger.Warn(
                                            $"{GetType().Name} ({nextJob.GetType().Name}): State remained {IoJobMeta.JobState.Producing}");
                                        nextJob.State = IoJobMeta.JobState.Error;
                                    }
#endif

                                    //Handle failure that leads to teardown
                                    if (nextJob.State == IoJobMeta.JobState.Cancelled ||
                                        nextJob.State == IoJobMeta.JobState.ProdCancel ||
                                        nextJob.State == IoJobMeta.JobState.Error)
                                    {
                                        _logger.Trace(
                                            $"{nameof(ProduceAsync)}: [FAILED], s = `{nextJob}', {Description}");

                                        //FreeAsync job
                                        nextJob.State = IoJobMeta.JobState.Reject;
                                        nextJob = await FreeAsync(nextJob, true).FastPath().ConfigureAwait(false);
                                        IsArbitrating = false;

                                        //TEARDOWN
                                        await ZeroAsync(this).FastPath().ConfigureAwait(false);
                                        return false;
                                    }

                                    //Is the producer spinning?
                                    if (_producerStopwatch.ElapsedMilliseconds < parm_min_failed_production_time)
                                        await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token)
                                            .ConfigureAwait(false);

                                    //FreeAsync job
                                    nextJob.State = IoJobMeta.JobState.Reject;
                                    nextJob = await FreeAsync(nextJob, true).FastPath().ConfigureAwait(false);

                                    // Signal prefetch back pressure
                                    if (nextJob.Source.PrefetchEnabled && enablePrefetchOption)
                                        nextJob.Source.PrefetchPressure();

                                    //signal back pressure
                                    Source.BackPressure();
                                }
                            }
                        }
                        else
                        {
                            if (nextJob != null)
                            {
                                nextJob.State = IoJobMeta.JobState.ProdCancel;
                                nextJob = await FreeAsync(nextJob, true).FastPath().ConfigureAwait(false);
                            }

                            //Are we in teardown?
                            if (Zeroed())
                            {
                                return false;
                            }

                            _logger.Warn(
                                $"{GetType().Name}: Production for: `{Description}` failed. Cannot allocate job resources!, heap =>  {JobHeap.CurrentHeapSize}/{JobHeap.MaxSize}");
                            await Task.Delay(parm_error_timeout, AsyncTasks.Token).ConfigureAwait(false);
                            return false;
                        }
                    }
                    catch (NullReferenceException e) when (!Zeroed())
                    {
                        _logger.Trace(e, $"{Description}");
                    }
                    catch (ObjectDisposedException e) when (!Zeroed())
                    {
                        _logger.Trace(e, $"{Description}");
                    }
                    catch (TimeoutException e) when (!Zeroed())
                    {
                        _logger.Trace(e, $"{Description}");
                    }
                    catch (TaskCanceledException e) when (!Zeroed())
                    {
                        _logger.Trace(e, $"{Description}");
                    }
                    catch (OperationCanceledException e) when (!Zeroed())
                    {
                        _logger.Trace(e, $"{Description}");
                    }
                    catch (Exception e)
                    {
                        if(!Zeroed())
                            _logger?.Error(e, $"{GetType().Name}: Producing `{Description}' returned with errors:");
                        
                        nextJob = await FreeAsync(nextJob, true).FastPath().ConfigureAwait(false);
                        return false;
                    }
                    finally
                    {
                        //prevent leaks
                        if (nextJob != null)
                        {
                            if(!(Zeroed() || nextJob.Zeroed()))
                                _logger.Error($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed..., state = {nextJob.State}");

                            //TODO Double check this hack
                            if (nextJob.State != IoJobMeta.JobState.Finished)
                            {
                                //_logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job status should be {nameof(IoJobMeta.CurrentState.Finished)}");
                                nextJob.State = IoJobMeta.JobState.Reject;
                            }
                            
                            await FreeAsync(nextJob, true).FastPath().ConfigureAwait(false);
                        }
                    }
                }
                else //We have run out of buffer space. Wait for the consumer to catch up
                {
                    if (enablePrefetchOption)
                    {
                        _logger.Warn($"{GetType().Name}: Source for `{Description}' is waiting for consumer to catch up! parm_max_q_size = `{parm_max_q_size}'");
                        await Task.Delay(parm_producer_consumer_throttle_delay, AsyncTasks.Token).ConfigureAwait(false);
                    }
                }                
            }

            catch(TaskCanceledException e) { _logger?.Trace(e,Description); }
            catch(OperationCanceledException e){ _logger?.Trace(e,Description); }
            catch(ObjectDisposedException e){ _logger?.Trace(e,Description); }
            catch(NullReferenceException e){ _logger?.Trace(e,Description); }
            catch(Exception e)
            {
                _logger?.Fatal(e, $"{GetType().Name}: {Description??"N/A"}: ");
            }
            finally
            {

                //_logger.Fatal($"{nameof(ProduceAsync)}: `{Description}' [EXIT]");
            }

            return false;
        }

        private async ValueTask<IoSink<TJob>> FreeAsync(IoSink<TJob> job, bool parent = false)
        {
            try
            {
                if (job == null)
                    return null;

                if (SyncRecoveryModeEnabled && job.PreviousJob != null)
                {
#if DEBUG
                    //if (((IoSink<TJob>)job.PreviousJob).State != IoJobMeta.JobState.Finished)
                    //{
                    //    _logger.Warn($"{GetType().Name}: PreviousJob fragment state = {((IoSink<TJob>)job.PreviousJob).State}");
                    //}
#endif

                    //TODO I don't think this is going to work
                    if(!job.PreviousJob.Syncing)
                        await JobHeap.ReturnAsync((IoSink<TJob>)job.PreviousJob, job.PreviousJob.FinalState != IoJobMeta.JobState.Accept).FastPath().ConfigureAwait(false);
                    else
                        await _previousJobFragment.PushAsync((IoSink<TJob>) job.PreviousJob).FastPath().ConfigureAwait(false);

                    job.PreviousJob = null;
                    return null;
                }
                //else if (!parent && job.CurrentState.Previous.CurrentState == IoJobMeta.CurrentState.Accept)
                //{
                //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
                //}

                if (parent || !SyncRecoveryModeEnabled)
                    await JobHeap.ReturnAsync(job, job.FinalState != IoJobMeta.JobState.Accept).FastPath().ConfigureAwait(false); ;
                
            }
            catch (NullReferenceException e)
            {
                _logger.Fatal(e,Description);
            }

            return null;
        }

        private long _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        private ValueTask _producerTask;
        private ValueTask _consumerTask;
        
        /// <summary>
        /// Consume 
        /// </summary>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="nanite"></param>
        /// <returns>True if consumption happened</returns>
        public async ValueTask<bool> ConsumeAsync<T>(Func<IoSink<TJob>, T, ValueTask> inlineCallback = null, T nanite = default)
        {
            try
            {
                //Waiting for a job to be produced. Did production fail?
                //if (_queue.IsEmpty) //TODO autoresetevent
                {
                    //wait for the producer pressure
                    //Wait for producer pressure
                    if (!await Source.WaitForPressureAsync().FastPath().ConfigureAwait(false))
                    {
                        //Was shutdown requested?
                        if (Zeroed() || AsyncTasks.IsCancellationRequested)
                        {
                            _logger.Trace($"{GetType().Name}: Consumer {Description} is shutting down");
                            return false;
                        }

                        //wait...
                        _logger.Trace($"{GetType().Name}: Consumer {Description} [[ProducerPressure]] timed out waiting on {Description}, willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                        await Task.Delay(parm_consumer_wait_for_producer_timeout / 4, AsyncTasks.Token).ConfigureAwait(false);

                        //Try again
                        return false;    
                    }
                }

                //A job was produced. Dequeue it and process
                var curJob = await _queue.DequeueAsync().FastPath().ConfigureAwait(false);
                if (!Zeroed() && (curJob != null))
                {
                    curJob.State = IoJobMeta.JobState.Consuming;
                    try
                    {

                        //sync previous failed job buffers
                        if (SyncRecoveryModeEnabled)
                            curJob.SyncPrevJob();

                        //Consume the job
                        if (await curJob.ConsumeAsync().FastPath().ConfigureAwait(false) ==
                            IoJobMeta.JobState.Consumed ||
                            curJob.State == IoJobMeta.JobState.ConInlined ||
                            curJob.State == IoJobMeta.JobState.FastDup
                            &&
                            !Zeroed())
                        {

                            //if (inlineCallback != null)
                            if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob, nanite).FastPath().ConfigureAwait(false);
                            }

                            curJob.State = IoJobMeta.JobState.Consumed;

                            Source.BackPressure();

                            //sync previous failed job buffers
                            if (SyncRecoveryModeEnabled)
                                curJob.JobSync();
                        }
                        else if (!Zeroed() && !curJob.Zeroed() && !Source.Zeroed())
                        {
                            _logger.Debug(
                                $"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' was unsuccessful, state = {curJob.State}");
                            curJob.State = IoJobMeta.JobState.Error;
                        }
                    }
                    catch (TaskCanceledException e) when (!Zeroed())
                    {
                        _logger?.Trace(e, Description);
                    }
                    catch (NullReferenceException e) when (!Zeroed())
                    {
                        _logger?.Trace(e, Description);
                    }
                    catch (ArgumentNullException e) when (!Zeroed())
                    {
                        _logger?.Trace(e.InnerException ?? e,
                            $"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' returned with errors:");
                    }
                    catch (Exception) when(Zeroed()){}
                    catch (Exception e)
                    {
                        _logger?.Error(e.InnerException ?? e,
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
                            
                            if (curJob.Id > 0 && curJob.Id % parm_stats_mod_count == 0 &&
                                _lastStat.ElapsedDelta() > TimeSpan.FromSeconds(10).TotalSeconds)
                            {
                                lock (Environment.Version)
                                {
                                    _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                    //_logger.Info(
                                    //    "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                    _logger.Info(
                                        $"{Description} {0/*JobHeap.IoFpsCounter.Fps()*/:F} j/s, [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.ReferenceCount + JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                    curJob.Source.PrintCounters();
                                    //_logger.Info("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                                }
                            }
                                
                            curJob = await FreeAsync(curJob).FastPath().ConfigureAwait(false);
                        }
                        catch
                        {
                            // ignored
                        }
                    }

                    return true;
                }
                return false;
            }
            catch (NullReferenceException e) { _logger?.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger?.Trace(e, Description); }
            catch (TimeoutException e) { _logger?.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger?.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger?.Trace(e, Description); }
            catch (Exception e)
            {
                _logger?.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }

            return false;
        }

        /// <summary>
        /// Starts the processors
        /// </summary>
        public virtual async Task AssimilateAsync()
        {
            _logger.Trace($"{GetType().Name}: Assimulating {Description}");
            
            //Producer
            _producerTask = ZeroAsyncOption(static async @this =>
            {
                //While supposed to be working
                try
                {
                    //Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                    var produceTask = new ValueTask<bool>[@this.ZeroConcurrencyLevel()];
                    while (!@this.Zeroed())
                    {
                        for (var i = 0; i < @this.ZeroConcurrencyLevel(); i++)
                        {
                            try
                            {
                                produceTask[i] = @this.ProduceAsync();
                            }
                            catch (Exception e)
                            {
                                @this._logger.Error(e, $"Production failed [{i}]: {@this.Description}");
                                return;
                            }
                        }

                        for (var i = 0; i < @this.ZeroConcurrencyLevel(); i++)
                        {
                            try
                            {
                                if (!await produceTask[i].FastPath().ConfigureAwait(false))
                                    return;
                            }
                            catch (Exception e)
                            {
                                @this._logger.Error(e, $"Production failed! {@this.Description}");
                                return;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    @this._logger.Error(e, $"Production failed! {@this.Description}");
                }
            },this, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.HideScheduler);

            //Consumer
            _consumerTask = ZeroAsyncOption(static async @this =>
            {
                //Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
                //While supposed to be working
                while (!@this.Zeroed())
                {
                    var consumeTaskPool = new ValueTask<bool>[@this.ZeroConcurrencyLevel()];
                    for (var i = 0; i < @this.ZeroConcurrencyLevel(); i++)
                    {
                        try
                        {
                            consumeTaskPool[i] = @this.ConsumeAsync<object>();
                        }
                        catch (Exception e)
                        {
                            @this._logger.Error(e, $"Consumption failed {@this.Description}");
                            return;
                        }
                    }

                    for (var i = 0; i < @this.ZeroConcurrencyLevel(); i++)
                    {
                        try
                        {
                            if (!await consumeTaskPool[i].FastPath().ConfigureAwait(false))
                                return;
                        }
                        catch (Exception e)
                        {
                            @this._logger.Error(e, $"Consumption failed {@this.Description}");
                            return;
                        }
                    }

                }
            }, this, TaskCreationOptions.AttachedToParent | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness, TaskScheduler.Default, true);

            //Wait for tear down                
            await Task.WhenAll(_producerTask.AsTask(), _consumerTask.AsTask()).ConfigureAwait(false);

            _logger.Trace($"{GetType().Name}: Processing for {Description} stopped");
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
