using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;

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
        /// <param name="enableZeroRecovery"></param>
        /// <param name="cascadeOnSource">If the source zeroes out, so does this <see cref="IoZero{TJob}"/> instance</param>
        /// <param name="concurrencyLevel"></param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IIoNanite, IoSink<TJob>> mallocJob,
            bool enableZeroRecovery, bool cascadeOnSource = true, int concurrencyLevel = 1) : base($"{nameof(IoSink<TJob>)}", concurrencyLevel < 0? source.ZeroConcurrencyLevel() : concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();

            ZeroRecoveryEnabled = enableZeroRecovery;
            //source.AsyncEnabled &= !ZeroRecoveryEnabled;

            ConfigureAsync(description, source, mallocJob, cascadeOnSource).AsTask().GetAwaiter();

            _zeroSync = new IoManualResetValueTaskSource<bool>();

            //What to do when certain parameters change
            //SettingChangedEvent += (sender, pair) =>
            //{
            //    //update heap to match max Q size
            //    if (pair.Key == nameof(parm_max_q_size))
            //    {
            //        JobHeap.Capacity = parm_max_q_size;
            //    }
            //};

            //prepare a multicast subject
            //ObservableRouter = this.Publish();
            //ObservableRouter.Connect();

            //Configure cancellations
            //AsyncTasks.Token.Register(() => ObservableRouter.Connect().Dispose());         
        }

        /// <summary>
        /// Configures the source.
        /// </summary>
        /// <param name="description">A description of the source</param>
        /// <param name="source">An instance of the source</param>
        /// <param name="jobMalloc"></param>
        /// <param name="cascade">Cascade to zero this if the source is zeroed</param>
        private async ValueTask ConfigureAsync(string description, IoSource<TJob> source,
            Func<object, IIoNanite, IoSink<TJob>> jobMalloc, bool cascade = false)
        {
            _description = description;
            
            Source = source ?? throw new ArgumentNullException($"{nameof(source)}");

            var capacity = (Source.PrefetchSize + 1) * 2;

            if (ZeroRecoveryEnabled)
                capacity *= 2;

            JobHeap = new IoHeapIo<IoSink<TJob>>($"{nameof(JobHeap)}: {_description}", capacity) {
                Malloc = jobMalloc, 
                Constructor = (sink, zero) =>
                {
                    sink.IoZero = (IoZero<TJob>)zero;
                }
            };

            if (cascade)
                await Source.ZeroHiveAsync(this).FastPath().ConfigureAwait(Zc);

            //TODO tuning
            _queue = new IoQueue<IoSink<TJob>>($"zero Q: {_description}", capacity, Source.PrefetchSize, disablePressure:!Source.DisableZero, enableBackPressure:Source.DisableZero);

            //TODO tuning
            if (ZeroRecoveryEnabled)
                _previousJobFragment = new IoQueue<IoSink<TJob>>($"{description}", capacity, ZeroConcurrencyLevel());
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
        //protected bool ZeroRecoveryEnabled => ConsumerCount == 1;
        public bool ZeroRecoveryEnabled { get; protected set; }

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
        /// Syncs producer and consumer queues
        /// </summary>
        private IoManualResetValueTaskSource<bool> _zeroSync;

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
        public int parm_max_q_size = 128; //TODO

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
        public int parm_stats_mod_count = 10000000;
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
        public int parm_min_failed_production_time = 250;

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

            await Source.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath().ConfigureAwait(Zc);

            await _queue.ZeroManagedAsync(static async (sink, @this) => await sink.Zero(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath().ConfigureAwait(@this.Zc), this,zero: true).FastPath().ConfigureAwait(Zc);

            await JobHeap.ZeroManagedAsync(static async (sink, @this) =>
            {
                await sink.Zero(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath().ConfigureAwait(@this.Zc);
            },this).FastPath().ConfigureAwait(Zc);

            if (_previousJobFragment != null)
            {
                await _previousJobFragment.ZeroManagedAsync(static (sink, @this) => sink.Zero(@this, $"{nameof(ZeroManagedAsync)}: teardown"), this, zero: true).FastPath().ConfigureAwait(Zc);
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
            IoSink<TJob> nextJob = null;
            try
            {
                //And the consumer is keeping up, which it should
                try
                {
                    nextJob = await JobHeap.TakeAsync(null, this).FastPath().ConfigureAwait(Zc);

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
#if DEBUG
                            //sanity check _previousJobFragment
                            if (ZeroRecoveryEnabled &&
                                _previousJobFragment.Count >= _previousJobFragment.Capacity * 2 / 3)
                            {
                                _logger.Warn(
                                    $"({GetType().Name}<{typeof(TJob).Name}>) {nameof(_previousJobFragment)} has grown large {_previousJobFragment.Count}/{_previousJobFragment.Capacity} ");
                            }
#endif

                        var ts = Environment.TickCount;
                        //Produce job input
                        if (await nextJob.ProduceAsync(static async (job, @this) =>
                            {
                                //Block on producer back pressure
                                if (await job.Source.WaitForBackPressureAsync().FastPath().ConfigureAwait(@this.Zc)) 
                                    return true;

                                job.State = IoJobMeta.JobState.ProduceErr;
                                return false;
                            }, this).FastPath().ConfigureAwait(Zc) == IoJobMeta.JobState.Produced && !Zeroed())
                        {
#if DEBUG
                            if (_queue.Count > 1 && _queue.Count > _queue.Capacity * 2 / 3)
                                _logger.Warn($"[[ENQUEUE]] backlog = {_queue.Count}/{_queue.Capacity}, {nextJob.Description}, {Description}");
#endif
                            //Enqueue the job for the consumer
                            nextJob.State = IoJobMeta.JobState.Queued;

                            if (await _queue.EnqueueAsync(nextJob, static async state =>
                                {
                                    var (@this, nextJob) = state;
                                    if (nextJob.Id == -1)
                                        nextJob.GenerateJobId();

                                    if (@this.ZeroRecoveryEnabled)
                                    {
                                        nextJob.PrevJobQHook = await @this._previousJobFragment
                                            .EnqueueAsync(nextJob).FastPath().ConfigureAwait(@this.Zc);
                                    }
                                }, (this, nextJob)).FastPath().ConfigureAwait(Zc) == null || nextJob.Source == null)
                            {
                                nextJob.State = IoJobMeta.JobState.ProduceErr;
                                nextJob.Source?.BackPressure();
                                await ZeroJobAsync(nextJob, true).FastPath().ConfigureAwait(Zc);
                                nextJob = null;

                                //how long did this failure take?
                                ts = ts.ElapsedMs();
                                IsArbitrating = false;

                                //Is the producer spinning? Slow it down
                                int throttleTime;
                                if ((throttleTime = parm_min_failed_production_time - ts) > 0)
                                    await Task.Delay(throttleTime, AsyncTasks.Token).ConfigureAwait(Zc);

                                return true;  //maybe we retry instead of crashing the producer
                            }

                            //Pass control over to the consumer
                            nextJob = null;

                            //Signal to the consumer that there is work to do
                            Source.Pressure();

                            //Fetch more work
                            Source.PrefetchPressure();

                            if (!IsArbitrating)
                                IsArbitrating = true;

                            return true;
                        }
                        else //produce job returned with errors or nothing...
                        {
                            //how long did this failure take?
                            ts = ts.ElapsedMs();
                            IsArbitrating = false;

                            await ZeroJobAsync(nextJob, true).FastPath().ConfigureAwait(Zc);
                            nextJob = null;

                            //signal back pressure
                            Source.BackPressure();

                            // prefetch pressure
                            Source.PrefetchPressure();

                            //Are we in teardown?
                            if (Zeroed())
                                return false;

                            //Is the producer spinning? Slow it down
                            int throttleTime;
                            if((throttleTime = parm_min_failed_production_time - ts) > 0)
                                await Task.Delay(throttleTime, AsyncTasks.Token).ConfigureAwait(Zc);
                            
                            return true; //maybe we retry instead of crashing the producer
                        }
                    }
                    else
                    {
                        //Are we in teardown?
                        if (Zeroed())
                            return false;

                        _logger.Warn(
                            $"{GetType().Name}: Production for: {Description} failed. Cannot allocate job resources!, heap =>  {JobHeap.Count}/{JobHeap.Capacity}");
                        await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token).ConfigureAwait(Zc);

                        return false;
                    }
                }
                catch (Exception) when (Zeroed())
                {
                }
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
                            _logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed..., state = {nextJob.State}");

                        await ZeroJobAsync(nextJob, true).FastPath().ConfigureAwait(Zc);
                        nextJob = null;
                    }
                }
            }
            catch (Exception) when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Fatal(e, $"{GetType().Name}: {Description ?? "N/A"}: ");
            }
            finally
            {
                if (nextJob != null)
                    throw new ApplicationException($"{nextJob} not freed!!!");
            }

            return false;
        }

        /// <summary>
        /// Returns a job back to the heap after use. Jobs may be combined with previous ones 
        /// so we don't return the current job to the heap. Only the previous one which
        /// should be completely processed at this stage. 
        /// </summary>
        /// <param name="job">The job to return to the heap</param>
        /// <param name="purge">Purge this job from recovery bits</param>
        /// <returns>The job</returns>
        private ValueTask ZeroJobAsync(IoSink<TJob> job, bool purge = false)
        {
            IoSink<TJob> prevJob = null;
            try
            {
                if (job == null)
                    return default;

                if (!ZeroRecoveryEnabled)
                    JobHeap.Return(job, job.FinalState > 0 && job.FinalState != IoJobMeta.JobState.Accept);
                else
                {
                    try
                    {
                        job.ZeroRecovery.SetResult(job.ZeroEnsureRecovery());
                    }
                    catch
                    {
                        // ignored
                    }

                    if (job.PreviousJob != null)
                    {
                        prevJob = (IoSink<TJob>)job.PreviousJob;
                        job.PreviousJob = null;
                        JobHeap.Return(prevJob, prevJob.FinalState > 0 && prevJob.FinalState != IoJobMeta.JobState.Accept);
                    }

                    if (purge)
                    {
                        if (job.PrevJobQHook != null)
                        {
                            _previousJobFragment.RemoveAsync(job.PrevJobQHook).FastPath().ConfigureAwait(Zc);
                            job.PrevJobQHook = null;
                        }
                        JobHeap.Return(job, true);
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e,$"p = {prevJob}, s = {job?.FinalState}");
            }

            return default;
        }

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
            IoSink<TJob> curJob = null;
            try
            {
                //Wait for producer pressure
                if (!await Source.WaitForPressureAsync().FastPath().ConfigureAwait(Zc))
                    return false;

                //A job was produced. Dequeue it and process
                curJob = await _queue.DequeueAsync().FastPath().ConfigureAwait(Zc);

                if (curJob is not { State: IoJobMeta.JobState.ProduceTo })
                {
#if DEBUG
                    if (_queue.Count > 1 && _queue.Count > _queue.Capacity * 2 / 3)
                        _logger.Warn($"[[DEQUEUE]] backlog = {_queue.Count}, {curJob.Description}, {Description}");
#endif
                    try
                    {
                        curJob.State = IoJobMeta.JobState.Consuming;

                        //Sync previous fragments into this job
                        if (ZeroRecoveryEnabled)
                        {
                            _previousJobFragment.Modified = false;
                            var cur = _previousJobFragment.Head;
                            var c = _previousJobFragment.Count * 2;
                            while (cur != null && c-- > 0)
                            {
                                if (_previousJobFragment.Modified)
                                {
                                    _previousJobFragment.Modified = false;
                                    cur = _previousJobFragment.Head;
                                    continue;
                                }

                                try
                                {
                                    if (cur.Value.Id == curJob.Id - 1)
                                    {
                                        curJob.PreviousJob = cur.Value;
                                        await _previousJobFragment.RemoveAsync(cur).FastPath().ConfigureAwait(Zc);
                                        curJob.PrevJobQHook = null;
                                    }
                                    else if (cur.Value.Id < curJob.Id - Source.PrefetchSize - 1)
                                    {
                                        var job = cur.Value;
                                        await _previousJobFragment.RemoveAsync(cur).FastPath().ConfigureAwait(Zc);
                                        job.PrevJobQHook = null;
                                        await ZeroJobAsync(job, true).FastPath().ConfigureAwait(Zc);
                                    }

                                    cur = cur.Next;
                                }
                                catch
                                {
                                    _previousJobFragment.Modified = false;
                                    cur = _previousJobFragment.Head;
                                }
                            }
                        }

                        //Consume the job
                        if (await curJob.ConsumeAsync().FastPath().ConfigureAwait(Zc) == IoJobMeta.JobState.Consumed || curJob.State is IoJobMeta.JobState.ConInlined or IoJobMeta.JobState.FastDup)
                        {
                            //if (inlineCallback != null)
                            if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob, nanite).FastPath().ConfigureAwait(Zc);
                            }

                            //count the number of work done
                            IncEventCounter();
                        }
                        else if (curJob.State != IoJobMeta.JobState.RSync &&
                                 curJob.State != IoJobMeta.JobState.ZeroRecovery && 
                                 curJob.State != IoJobMeta.JobState.Fragmented &&
                                 curJob.State != IoJobMeta.JobState.BadData && !Zeroed() && !curJob.Zeroed())
                        {
                            _logger.Error($"{Description}: {curJob.TraceDescription} consuming job: {curJob.Description} was unsuccessful, state = {curJob.State}");
                        }
                    }
                    catch (Exception) when (Zeroed() || curJob.Zeroed())
                    {
                    }
                    catch (Exception e) when (!Zeroed() && !curJob.Zeroed())
                    {
                        _logger?.Error(e,
                            $"{Description}: {curJob.TraceDescription} consuming job: {curJob.Description} returned with errors:");
                    }
                    finally
                    {
                        //Consume success?
                        
                        curJob.State = curJob.State is IoJobMeta.JobState.Consumed or IoJobMeta.JobState.Fragmented
                            ? IoJobMeta.JobState.Accept
                            : IoJobMeta.JobState.Reject;
                        try
                        {
                            if (curJob.Id % parm_stats_mod_count == 0 && curJob.Id >= 9999)
                            {
                                await ZeroAtomic(static (_, state, _) =>
                                {
                                    var (@this, curJob) = state;
                                    @this._logger?.Info(
                                        $"{@this.Description} {@this.EventCount / (double)@this.UpTime.ElapsedMsToSec():0.0} j/s, [{@this.JobHeap.ReferenceCount} / {@this.JobHeap.CacheSize} / {@this.JobHeap.ReferenceCount + @this.JobHeap.CacheSize} / {@this.JobHeap.AvailableCapacity} / {@this.JobHeap.Capacity}]");
                                    curJob.Source.PrintCounters();
                                    return new ValueTask<bool>(true);
                                }, (this, curJob)).FastPath().ConfigureAwait(Zc);
                            }

                            await ZeroJobAsync(curJob).FastPath().ConfigureAwait(Zc);
                            curJob = null;
                            Source.BackPressure();
                        }
                        catch when (Zeroed())
                        {
                        }
                        catch (Exception e) when (!Zeroed())
                        {
                            _logger?.Fatal(e);
                        }
                    }

                    return true;
                }

                await ZeroJobAsync(curJob, true).FastPath().ConfigureAwait(Zc);
                curJob = null;
                Source.BackPressure();

                return false;
            }
            catch (Exception) when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }
            finally
            {
                if (curJob != null)
                    throw new ApplicationException($"{curJob} not freed!!!");
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
                    var width = @this.Source.PrefetchSize;
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

                        var waitForConsumer = new ValueTask<bool>(@this._zeroSync, @this._zeroSync.Version);
                        if (!await waitForConsumer.FastPath().ConfigureAwait(@this.Zc))
                            break;
                        @this._zeroSync.Reset();
                    }
                }
                catch when(@this.Zeroed()){}
                catch (Exception e) when(!@this.Zeroed())
                {
                    @this._logger.Error(e, $"Production failed! {@this.Description}");
                }
            },this, TaskCreationOptions.LongRunning, IoZeroScheduler.ZeroDefault); //TODO tuning

            //Consumer
            _consumerTask = ZeroOptionAsync(static async @this =>
            {
                var width = @this.Source.PrefetchSize;
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

                    @this._zeroSync.SetResult(j == width);
                    if (j < width)
                        break;
                }
            }, this, TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness, IoZeroScheduler.ZeroDefault); //TODO tuning

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
