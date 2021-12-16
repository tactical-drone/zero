using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.core.patterns.bushings
{
    /// <summary>
    /// Producer/Consumer pattern
    /// </summary>    
    /// <typeparam name="TJob">The type of job</typeparam>
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
        /// <param name="cascadeOnSource">If the source zeroes out, so does this <see cref="IoZero{TJob}"/> instance</param>
        /// <param name="concurrencyLevel"></param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IIoNanite, IoSink<TJob>> mallocJob,
            bool enableSync, bool cascadeOnSource = true, int concurrencyLevel = 1) : base($"{nameof(IoSink<TJob>)}", concurrencyLevel < 0? source.ZeroConcurrencyLevel() : concurrencyLevel)
        {
            ConfigureAsync(description, source, mallocJob, cascadeOnSource).AsTask().GetAwaiter();

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

            //TODO tuning
            if (SyncRecoveryModeEnabled)
                _previousJobFragment = new IoQueue<IoSink<TJob>>($"{description}", ZeroConcurrencyLevel()*10, ZeroConcurrencyLevel());
        }

        /// <summary>
        /// Configures the source.
        /// </summary>
        /// <param name="description">A description of the source</param>
        /// <param name="source">An instance of the source</param>
        /// <param name="mallocMessage"></param>
        /// <param name="cascade">Cascade to zero this if the source is zeroed</param>
        private async ValueTask ConfigureAsync(string description, IoSource<TJob> source,
            Func<object, IIoNanite, IoSink<TJob>> mallocMessage, bool cascade = false)
        {
            _description = description;
            
            Source = source ?? throw new ArgumentNullException($"{nameof(source)}");

            JobHeap = new IoHeapIo<IoSink<TJob>>($"{nameof(JobHeap)}: {_description}", Source.PrefetchSize + 2) { Malloc = mallocMessage };

            if (cascade)
                await Source.ZeroHiveAsync(this).FastPath().ConfigureAwait(Zc);

            //TODO tuning
            _queue = new IoQueue<IoSink<TJob>>($"zero Q: {_description}", (ZeroConcurrencyLevel() * 2 + Source.PrefetchSize), Source.PrefetchSize, disablePressure:!Source.DisableZero, enableBackPressure:Source.DisableZero);
        }

        /// <summary>
        /// The source of the work
        /// </summary>
        public IoSource<TJob> Source { get; protected set; }

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
        /// UpstreamSource <see cref="Source"/> reference
        /// </summary>
        public IIoSource IoSource => Source;

        /// <summary>
        /// The job queue
        /// </summary>
        private IoQueue<IoSink<TJob>> _queue;
        
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
        public bool IsArbitrating { get; private set; }

        /// <summary>
        /// Maintains a handle to a job if fragmentation was detected so that the
        /// source can marshal fragments into the next production
        /// </summary>
        private volatile IoQueue<IoSink<TJob>> _previousJobFragment;

        private long _eventCounter;
        /// <summary>
        /// An event counter
        /// </summary>
        public long EventCount => Interlocked.Read(ref _eventCounter);

        /// <summary>
        /// Maximum amount of producers that can be buffered before we stop production of new jobs
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_q_size = 20; //TODO

        /// <summary>
        /// Minimum useful uptime in seconds
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_min_uptime_ms = 2000;

        /// <summary>
        /// Debug output rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
#if DEBUG
        public int parm_stats_mod_count = 20000;
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
        public int parm_error_timeout = 1000;
        
        /// <summary>
        /// The amount of time to wait between retries when the source cannot allocate job management structures
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_conduit_spin_up_wait_time = 250;

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
        public int parm_min_failed_production_time = 1000;

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
        /// If we are zeroed or not
        /// </summary>
        /// <returns>true if zeroed, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed();
        }

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
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            Source.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown");

            await _queue.ZeroManagedAsync(static (sink, @this) =>
            {
                sink.Zero(@this, $"{nameof(ZeroManagedAsync)}: teardown");
                return default;
            }, this,zero: true).FastPath().ConfigureAwait(Zc);

            await JobHeap.ZeroManagedAsync(static (sink, @this) =>
            {
                sink.Zero(@this, $"{nameof(ZeroManagedAsync)}: teardown");
                return default;
            },this).FastPath().ConfigureAwait(Zc);

            if (_previousJobFragment != null)
            {
                await _previousJobFragment.ZeroManagedAsync(static (sink, @this) =>
                {
                    sink.Zero(@this, $"{nameof(ZeroManagedAsync)}: teardown");
                    return default;
                }, this, zero: true).FastPath().ConfigureAwait(Zc);
            }

#if DEBUG
            _logger.Trace($"Closed {Description} from {ZeroedFrom}: reason = {ZeroReason}");
#endif
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
                Debug.Assert(_queue.Count < parm_max_q_size);
                //And the consumer is keeping up, which it should

                IoSink<TJob> nextJob = null;
                try
                {
                    nextJob = await JobHeap.TakeAsync(localReuse: static (job, ioZero) =>
                    {
                        job.IoZero = ioZero;
                        return new ValueTask<IoSink<TJob>>(job);
                    }, this).FastPath().ConfigureAwait(Zc);

                    //Allocate a job from the heap
                    if (nextJob != null)
                    {
                        nextJob.State = IoJobMeta.JobState.Producing;

                        //wait for prefetch pressure
                        if (enablePrefetchOption)
                        {
                            if (!await nextJob.Source.WaitForPrefetchPressureAsync().FastPath().ConfigureAwait(Zc))
                                return false;
                        }

                        if (!Zeroed())
                        {
                            //sanity check _previousJobFragment
                            if (SyncRecoveryModeEnabled && _previousJobFragment.Count >= JobHeap.MaxSize * 2 / 3)
                            {
                                _logger.Warn(
                                    $"({GetType().Name}<{typeof(TJob).Name}>) {nameof(_previousJobFragment)} id = {nextJob.Id} has grown large, zeroing...");
                            }

                            IoSink<TJob> prevJobFragment;
                            if (SyncRecoveryModeEnabled && (prevJobFragment = await _previousJobFragment.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
                            {
                                //if( prevJobFragment.Id == nextJob.Id + 1)
                                nextJob.PreviousJob = prevJobFragment;
                                //_logger.Fatal($"{GetHashCode()}:{nextJob.GetHashCode()}: {nextJob.Id}");
                                nextJob.PrevJobQHook = await _previousJobFragment.EnqueueAsync(nextJob).FastPath().ConfigureAwait(Zc);
                            }

                            _producerStopwatch.Restart();

                            //prepare a failed job to sync with next job
                            if (SyncRecoveryModeEnabled)
                                nextJob.JobSync();

                            //Produce job input
                            if (await nextJob.ProduceAsync(static async (job, @this) =>
                                {
                                    //Block on producer back pressure
                                    if (!await job.Source.WaitForBackPressureAsync().FastPath().ConfigureAwait(@this.Zc))
                                    {
                                        job.State = IoJobMeta.JobState.ProduceErr;
                                        return false;
                                    }
                                    return true;
                                }, this).FastPath().ConfigureAwait(Zc) == IoJobMeta.JobState.Produced && !Zeroed())
                            {
                                _producerStopwatch.Stop();

#if DEBUG
                                if (_queue.Count > 1 && _queue.Count > _queue.Capacity * 2 / 3)
                                    _logger.Warn($"[[ENQUEUE]] backlog = {_queue.Count}/{_queue.Capacity}, {nextJob.Description}, {Description}");
#endif
                                //Enqueue the job for the consumer
                                nextJob.State = IoJobMeta.JobState.Queued;

                                if (await _queue.EnqueueAsync(nextJob).FastPath().ConfigureAwait(Zc) == null ||
                                    nextJob.Source == null)
                                {
                                    nextJob.State = IoJobMeta.JobState.ProduceErr;
                                    return true; //maybe we retry
                                }

                                if (enablePrefetchOption)
                                    Source.PrefetchPressure();

                                if (!IsArbitrating)
                                    IsArbitrating = true;

                                //Pass control over to the consumer
                                nextJob = null;

                                //Signal to the consumer that there is work to do
                                return await Source.PressureAsync().FastPath().ConfigureAwait(Zc) >
                                       0; //TODO, is this a good idea?
                            }
                            else //produce job returned with errors or nothing...
                            {
                                //how long did this failure take?
                                _producerStopwatch.Stop();
                                IsArbitrating = false;

                                // Signal prefetch back pressure
                                if (enablePrefetchOption && nextJob.Source is { PrefetchEnabled: true })
                                    nextJob.Source.PrefetchPressure();

                                //signal back pressure
                                Source.BackPressureAsync();

                                //Enqueue the job for the consumer so that it can stay in sync
                                //nextJob.State = IoJobMeta.JobState.ConCancel;

                                //if (await _queue.EnqueueAsync(nextJob).FastPath().ConfigureAwait(Zc) == null ||
                                //    nextJob.Source == null)
                                {
                                    nextJob.State = IoJobMeta.JobState.ProduceErr;

                                    //ReturnJobToHeapAsync job
                                    nextJob.State = IoJobMeta.JobState.Reject;
                                    await ReturnJobToHeapAsync(nextJob, true).FastPath()
                                        .ConfigureAwait(Zc);

                                    nextJob = null;

                                    //Are we in teardown?
                                    if (Zeroed())
                                        return false;

                                    //Is the producer spinning? Slow it down
                                    if (_producerStopwatch.ElapsedMilliseconds < parm_min_failed_production_time)
                                        await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token).ConfigureAwait(Zc);

                                    return true; //maybe we retry
                                }
                            }
                        }
                    }
                    else
                    {
                        //Are we in teardown?
                        if (Zeroed())
                            return false;

                        _logger.Warn($"{GetType().Name}: Production for: `{Description}` failed. Cannot allocate job resources!, heap =>  {JobHeap.Count}/{JobHeap.MaxSize}");
                        await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token).ConfigureAwait(Zc);
                        return false;
                    }
                }
                catch (Exception) when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    _logger?.Error(e, $"{GetType().Name}: Producing {Description} returned with errors:");
                    return false;
                }
                finally
                {
                    //prevent leaks
                    if (nextJob != null)
                    {
                        if (!Zeroed() && !nextJob.Zeroed())
                            _logger.Trace($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed..., state = {nextJob.State}");

                        //TODO Double check this hack
                        if (nextJob.State != IoJobMeta.JobState.Halted)
                        {
                            //_logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job status should be {nameof(IoJobMeta.CurrentState.Finished)}");
                            nextJob.State = IoJobMeta.JobState.Reject;
                        }

                        await ReturnJobToHeapAsync(nextJob, true).FastPath().ConfigureAwait(Zc);
                    }
                }
            }
            catch (Exception) when (Zeroed()) {}
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Fatal(e, $"{GetType().Name}: {Description ?? "N/A"}: ");
            }

            return false;
        }

        /// <summary>
        /// Returns a job back to the heap after use. Jobs may be combined with previous ones 
        /// so we don't return the current job to the heap. Only the previous one which
        /// should be completely processed at this stage. 
        /// </summary>
        /// <param name="job">The job to return to the heap</param>
        /// <param name="freeCurrent">If the current and previous job is to be returned</param>
        /// <returns>The job</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask ReturnJobToHeapAsync(IoSink<TJob> job, bool freeCurrent = false)
        {
            try
            {
                if (job == null)
                    return;

                if (!freeCurrent && SyncRecoveryModeEnabled && job.PreviousJob != null)
                {
#if DEBUG
                    //if (((IoSink<TJob>)job.PreviousJob).State != IoJobMeta.JobState.Finished)
                    //{
                    //    _logger.Warn($"{GetType().Name}: PreviousJob fragment state = {((IoSink<TJob>)job.PreviousJob).State}");
                    //}
#endif
                    //TODO I don't think this is going to work
                    if (!job.PreviousJob.Syncing)
                    {
                        var prevJob = ((IoSink<TJob>)job.PreviousJob);
                        if (prevJob.PrevJobQHook?.Value != null)
                        {
                            await _previousJobFragment.RemoveAsync(prevJob.PrevJobQHook).FastPath().ConfigureAwait(Zc);
                            prevJob.PrevJobQHook.Next = null;
                            prevJob.PrevJobQHook.Prev = null;
                            prevJob.PrevJobQHook.Value = default;
                        }

                        JobHeap.Return(prevJob, job.PreviousJob.FinalState != IoJobMeta.JobState.Accept);
                    }
                    else
                    {
                        await _previousJobFragment.PushBackAsync((IoSink<TJob>)job.PreviousJob).FastPath().ConfigureAwait(Zc);
                    }
                        

                    job.PreviousJob = null;
                }
                else
                {
                    if (job.PrevJobQHook?.Value != null)
                    {
                        await _previousJobFragment.RemoveAsync(job.PrevJobQHook).FastPath().ConfigureAwait(Zc);
                        job.PrevJobQHook.Next = null;
                        job.PrevJobQHook.Prev = null;
                        job.PrevJobQHook.Value = default;
                    }

                    JobHeap.Return(job, job.FinalState != IoJobMeta.JobState.Accept);
                }
                //else if (!parent && job.CurrentState.Previous.CurrentState == IoJobMeta.CurrentState.Accept)
                //{
                //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
                //}
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Fatal(e,Description);
            }
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
            //fail fast
            if (Zeroed())
                return false;

            try
            {
                //Wait for producer pressure
                if (!await Source.WaitForPressureAsync().FastPath().ConfigureAwait(Zc))
                {
                    //Was shutdown requested?
                    if (Zeroed())
                        return false;

                    //wait...
                    _logger.Trace(
                        $"{Description}: {nameof(Source.WaitForPressureAsync)} timed out waiting, willing to wait {parm_consumer_wait_for_producer_timeout}ms");
                    await Task.Delay(parm_consumer_wait_for_producer_timeout / 4, AsyncTasks.Token).ConfigureAwait(Zc);

                    //Try again
                    return false;
                }
                //A job was produced. Dequeue it and process
                var curJob = await _queue.DequeueAsync().FastPath().ConfigureAwait(Zc);

                if (curJob != null && curJob.State != IoJobMeta.JobState.ProdCancel)
                {
#if DEBUG
                    if (_queue.Count > 1 && _queue.Count > _queue.Capacity * 2/3)
                        _logger.Warn($"[[DEQUEUE]] backlog = {_queue.Count}, {curJob.Description}, {Description}");
#endif
                    try
                    {
                        curJob.State = IoJobMeta.JobState.Consuming;
                        //sync previous failed job buffers
                        if (SyncRecoveryModeEnabled)
                            curJob.SyncPrevJob();

                        //Consume the job
                        if (await curJob.ConsumeAsync().FastPath().ConfigureAwait(Zc) == IoJobMeta.JobState.Consumed ||
                            curJob.State is IoJobMeta.JobState.ConInlined or IoJobMeta.JobState.FastDup)
                        {
                            //if (inlineCallback != null)
                            if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob, nanite).FastPath().ConfigureAwait(Zc);
                                curJob.State = IoJobMeta.JobState.Consumed;
                            }

                            Source.BackPressureAsync();

                            //count the number of work done
                            IncEventCounter();

                            //sync previous failed job buffers
                            if (SyncRecoveryModeEnabled)
                                curJob.JobSync();
                        }
                        else if (!Zeroed() && !curJob.Zeroed())
                        {
                            
                            _logger.Error(
                                $"{Description}: {curJob.TraceDescription} consuming job: {curJob.Description} was unsuccessful, state = {curJob.State}");
                            curJob.State = IoJobMeta.JobState.Error;
                        }
                    }
                    catch (Exception) when (Zeroed())
                    {
                    }
                    catch (Exception e) when (!Zeroed())
                    {
                        _logger?.Error(e,
                            $"{Description}: {curJob.TraceDescription} consuming job: {curJob.Description} returned with errors:");
                    }
                    finally
                    {
                        //Consume success?
                        curJob.State = curJob.State is IoJobMeta.JobState.Consumed? IoJobMeta.JobState.Accept : IoJobMeta.JobState.Reject;

                        try
                        {
                            if (curJob.Id % parm_stats_mod_count == 0 && _lastStat.Elapsed() > 10)
                            {
                                await ZeroAtomic(static (_,state,_) =>
                                {
                                    var (@this, curJob) = state;
                                    @this._lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                    @this._logger?.Info(
                                        $"{@this.Description} {@this.JobHeap.OpsPerSecond:0.0} j/s, [{@this.JobHeap.ReferenceCount} / {@this.JobHeap.CacheSize()} / {@this.JobHeap.ReferenceCount + @this.JobHeap.CacheSize()} / {@this.JobHeap.FreeCapacity()} / {@this.JobHeap.MaxSize}]");
                                    curJob.Source.PrintCounters();
                                    return new ValueTask<bool>(true);
                                }, (this,curJob)).FastPath().ConfigureAwait(Zc);
                            }

                            await ReturnJobToHeapAsync(curJob).FastPath().ConfigureAwait(Zc);
                        }
                        catch when(Zeroed()){}
                        catch (Exception e) when(!Zeroed())
                        {
                            _logger?.Fatal(e);
                        }
                    }

                    return true;
                }

                Source.BackPressureAsync();
                await ReturnJobToHeapAsync(curJob, true).FastPath().ConfigureAwait(Zc);

                return false;
            }
            catch (Exception) when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }


            return false;
        }

        /// <summary>
        /// Starts processing work queues
        /// </summary>
        public virtual async ValueTask BlockOnReplicateAsync()
        {
            var desc = Description;
#if DEBUG
            _logger.Trace($"{GetType().Name}: Assimulating {desc}");
#endif
            //Producer
            _producerTask = ZeroOptionAsync(static async @this =>
            {
                try
                {
                    var width = @this.Source.ZeroConcurrencyLevel();
                    var preload = new ValueTask<bool>[width];
                    //While supposed to be working
                    while (!@this.Zeroed())
                    {
                        for (var i = 0; i < width; i++)
                        {
                            try
                            {
                                preload[i] = @this.ProduceAsync();
                            }
                            catch (Exception e)
                            {
                                @this._logger.Error(e, $"Production failed [{i}]: {@this.Description}");
                                break;
                            }
                        }

                        var j = 0;
                        while (await preload[j].FastPath() && ++j < width) { }

                        if (j < width)
                            break;
                    }
                }
                catch when(@this.Zeroed()){}
                catch (Exception e) when(!@this.Zeroed())
                {
                    @this._logger.Error(e, $"Production failed! {@this.Description}");
                }
            },this, TaskCreationOptions.DenyChildAttach); //TODO tuning

            //Consumer
            _consumerTask = ZeroOptionAsync(static async @this =>
            {
                var width = @this.Source.ZeroConcurrencyLevel();
                var preload = new ValueTask<bool>[width];
                //While supposed to be working
                while (!@this.Zeroed())
                {
                    for (var i = 0; i < width; i++)
                    {
                        try
                        {
                            preload[i] = @this.ConsumeAsync<object>();
                        }
                        catch when (@this.Zeroed()) { }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger.Error(e, $"Consumption failed {@this.Description}");
                            break;
                        }
                    }

                    var j = 0;
                    while (await preload[j].FastPath() && ++j < width) { }

                    if (j < width)
                        break;
                }
            }, this, TaskCreationOptions.DenyChildAttach); //TODO tuning

            //Wait for tear down                
            await Task.WhenAll(_producerTask.AsTask(), _consumerTask.AsTask()).ConfigureAwait(Zc);

#if DEBUG
            _logger?.Trace($"{GetType().Name}: Processing for {desc} stopped");
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncEventCounter()
        {
            Interlocked.Increment(ref _eventCounter);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ZeroEventCounter()
        {
            Interlocked.Exchange(ref _eventCounter, 0);
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
