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
using zero.core.patterns.semaphore;
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
            bool enableZeroRecovery, bool cascadeOnSource = true, int concurrencyLevel = 1) : base($"{nameof(IoSink<TJob>)}", concurrencyLevel < 0? source?.ZeroConcurrencyLevel()??0 : concurrencyLevel)
        {
            //sentinel
            if (source == null)
                return;

            _logger = LogManager.GetCurrentClassLogger();

            ZeroRecoveryEnabled = enableZeroRecovery;

            ConfigureAsync(description, source, mallocJob, cascadeOnSource).AsTask().GetAwaiter();

            //TODO tuning
            if (ZeroRecoveryEnabled)
                _previousJobFragment = new IoQueue<IoSink<TJob>>($"{description}", (Source.PrefetchSize + Source.ZeroConcurrencyLevel()) * 3, Source.PrefetchSize);

            _zeroSync = new IoManualResetValueTaskSource<bool>();
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
            Source = source;
            var capacity = Source.PrefetchSize + Source.ZeroConcurrencyLevel() + 1;

            //These numbers were numerically established
            if (ZeroRecoveryEnabled)
                capacity *= 3;

            try
            {
                //TODO tuning
                
                _queue = new IoZeroSemaphoreChannel<IoSink<TJob>>($"zero Q: {_description}",capacity, zeroAsyncMode:false);//FALSE
                

                JobHeap = new IoHeapIo<IoSink<TJob>>($"{nameof(JobHeap)}: {_description}", capacity, jobMalloc) {
                    Constructor = (sink, zero) =>
                    {
                        sink.IoZero = (IoZero<TJob>)zero;
                    }
                };
            }
            catch (Exception e)
            {
                _logger.Trace(e,Description);
            }

            if (cascade)
                await Source.ZeroHiveAsync(this).FastPath();
        }

        /// <summary>
        /// The source of the work
        /// </summary>
        public IoSource<TJob> Source { get; protected set; }

        /// <summary>
        /// If jobs use previous jobs for successful recoveries (split/damaged network traffic etc.)
        /// </summary>
        public bool ZeroRecoveryEnabled { get; protected set; }

        /// <summary>
        /// UpstreamSource <see cref="Source"/> reference
        /// </summary>
        public IIoSource IoSource => Source;

        /// <summary>
        /// The job queue
        /// </summary>
        private IoZeroSemaphoreChannel<IoSink<TJob>> _queue;

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
        private readonly ILogger _logger;

        /// <summary>
        /// Indicates whether jobs are being processed
        /// </summary>
        public bool IsArbitrating { get; private set; }

        /// <summary>
        /// Maintains a handle to a job if fragmentation was detected so that the
        /// source can marshal fragments into the next production
        /// </summary>
        private readonly IoQueue<IoSink<TJob>> _previousJobFragment;


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
        public int parm_stats_mod_count = 50000;
#else
        public int parm_stats_mod_count = 1000000;
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
        public int parm_error_popdog = 10000;
        
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
        public int parm_min_failed_production_time = 16 * 3;

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
        /// How long a source will sleep for when it is getting skipped productions
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_io_batch_size = 4;

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
            Source = null;
            JobHeap = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            if(Source != null) //TODO: how can this be?
                await Source.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

            _queue?.ZeroSem();

            if (JobHeap != null)
            {
                await JobHeap.ZeroManagedAsync(static async (sink, @this) =>
                {
                    await sink.DisposeAsync(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
                }, this).FastPath();
            }
            
            if (_previousJobFragment != null)
            {
                await _previousJobFragment.ZeroManagedAsync(static (sink, @this) => sink.Value.DisposeAsync(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath(), this, zero: true).FastPath();
            }

#if DEBUG
            if(UpTime.ElapsedUtcMs() > parm_min_uptime_ms)
                _logger.Trace($"Closed {Description}");
#endif
        }

        /// <summary>
        /// Produce
        /// </summary>
        /// <returns></returns>
        public async ValueTask<bool> ProduceAsync() 
        {
            IoSink<TJob> nextJob = null;
            
            try
            {
                //wait for prefetch pressure
                await Source.WaitForPrefetchPressureAsync().FastPath();

                //Allocate a job from the heap
                nextJob = await JobHeap.TakeAsync(null, this).FastPath();

                if (nextJob != null)
                {
                    await nextJob.SetStateAsync(IoJobMeta.JobState.Producing).FastPath();

#if DEBUG
                    //sanity check _previousJobFragment
                    if (ZeroRecoveryEnabled &&
                        _previousJobFragment.Count >= _previousJobFragment.Capacity * 3 / 4)
                    {
                        _logger.Warn(
                            $"({GetType().Name}<{typeof(TJob).Name}>) {nameof(_previousJobFragment)} has grown large {_previousJobFragment.Count}/{_previousJobFragment.Capacity} ");
                    }
#endif
                    var ts = Environment.TickCount;
                    //Produce job input
                    if (!Zeroed() && await nextJob.ProduceAsync(static async (job, @this) =>
                        {
                            //Block on producer back pressure
                            try
                            {
                                await job.Source.WaitForBackPressureAsync().FastPath();
                                return true;
                            }
                            catch when(@this.Zeroed() || job.Zeroed()){}
                            catch (Exception e) when (!@this.Zeroed() && !job.Zeroed())
                            {
                                @this._logger.Error(e,$"{@this.Description}");
                            }
                            return false;
                        }, this).FastPath() == IoJobMeta.JobState.Produced || nextJob.State == IoJobMeta.JobState.ProdConnReset)
                    {
                        if (ZeroRecoveryEnabled)
                        {
                            if ((nextJob.FragmentIdx = await _previousJobFragment.EnqueueAsync(nextJob).FastPath()) == null &&
                                _previousJobFragment.Count == _previousJobFragment.Capacity)
                            {
                                _logger.Warn($"Flushing previous job cash {_previousJobFragment.Count} items, {Description}");
                                await _previousJobFragment.ClearAsync().FastPath();
                            }
                            else if(nextJob.FragmentIdx == null)
                            {
                                
                            }
                        }

                        nextJob.GenerateJobId();

                        //Enqueue the job for the consumer
                        if(nextJob.State != IoJobMeta.JobState.ProdConnReset)
                            await nextJob.SetStateAsync(IoJobMeta.JobState.Queued).FastPath();

                        if(_queue.Release(nextJob, true) < 0)
                        {
                            ts = ts.ElapsedMs();

                            await nextJob.SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();
                            await ZeroJobAsync(nextJob, true).FastPath();
                            nextJob = null;

                            Source.BackPressure(zeroAsync: true);
                            
                            //IsArbitrating = false;
                            //Is the producer spinning? Slow it down
                            int throttleTime;
                            if ((throttleTime = parm_min_failed_production_time - ts) > 0)
                                await Task.Delay(throttleTime, AsyncTasks.Token);

                            if(!Zeroed())
                                _logger.Warn($"Producer stalled.... {Description}");

                            Source.PrefetchPressure(zeroAsync: false);

                            return true;  //maybe we retry instead of crashing the producer
                        }

                        //Pass control over to the consumer
                        nextJob = null;

                        //if (!IsArbitrating)
                        //    IsArbitrating = true;

                        //Fetch more work
                        Source.PrefetchPressure(zeroAsync: false);
                        return true;
                    }
                    else //produce job returned with errors or nothing...
                    {
                        //how long did this failure take?
                        IsArbitrating = false;

                        if (nextJob.State == IoJobMeta.JobState.ProdSkipped)
                            await nextJob.SetStateAsync(IoJobMeta.JobState.Accept).FastPath();

                        await ZeroJobAsync(nextJob, nextJob.FinalState != IoJobMeta.JobState.Accept).FastPath();
                        nextJob = null;

                        //todo inject handler for special states...

                        //Are we in teardown?
                        if (Zeroed())
                            return false;

                        //signal back pressure
                        Source.BackPressure(zeroAsync: true);

                        // prefetch pressure
                        Source.PrefetchPressure(zeroAsync: false);

                        return false;
                    }
                }
                else
                {
                    //Are we in teardown?
                    if (Zeroed() || JobHeap.Zeroed)
                        return false;

                    Source.BackPressure(zeroAsync: true);
                    // prefetch pressure
                    Source.PrefetchPressure(zeroAsync:false);
                    
                    _logger.Warn($"{GetType().Name}:Q = {Source.QueueStatus}, backlog = {_previousJobFragment?.Count},  Production for: {Description} failed. Cannot allocate job resources!, heap =>  {JobHeap.Count}/{JobHeap.Capacity}");
                    await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token);

                    return false;
                }
            }
            catch (Exception) when (Zeroed() || nextJob != null && nextJob.Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed() && nextJob != null)
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
                        _logger?.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed..., state = {nextJob.State}");

                    if(!Zeroed())
                        await ZeroJobAsync(nextJob, true).FastPath();
                    nextJob = null;
                }
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
        private async ValueTask ZeroJobAsync(IoSink<TJob> job, bool purge = false)
        {
            IoSink<TJob> prevJob = null;
            try
            {
                if (job == null || Zeroed())
                    return;

                if (purge && job.FinalState == 0)
                {
#if DEBUG
                    _logger.Fatal($"sink purged -> {job.State}, {job.Description}, {Description}");
#endif
                }
                else if (job.FinalState == 0)
                {
                    _logger.Fatal($"sink un accounted for -> {job.Description}, {Description}");
                }

                if (!ZeroRecoveryEnabled)
                {
                    JobHeap.Return(job, job.FinalState > 0 && job.FinalState != IoJobMeta.JobState.Accept);
                }
                else
                {
                    try
                    {
                        job.ZeroRecovery.SetResult(true);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e,Description);
                        // ignored
                    }

                    if (purge)
                    {
                        var latch = job.FragmentIdx;
                        var qid = job.FragmentIdx?.Qid ?? 0;
                        if (latch != null && Interlocked.CompareExchange(ref job.FragmentIdx, null, latch) == latch)
                            await _previousJobFragment.RemoveAsync(latch, qid).FastPath();

                        JobHeap.Return(job, true);
                    }
                    else
                    {
                        if (job.PreviousJob != null)
                            JobHeap.Return((IoSink<TJob>)job.PreviousJob);
                        else
                        {
                            var latch = job.FragmentIdx;
                            var qid = job.FragmentIdx?.Qid ?? 0;
                            if (latch != null && Interlocked.CompareExchange(ref job.FragmentIdx, null, latch) == latch)
                                await _previousJobFragment.RemoveAsync(latch, qid).FastPath();

                            JobHeap.Return(job);
                        }
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e,$"p = {prevJob}, s = {job?.FinalState}");
            }
        }

        /// <summary>
        /// Consume 
        /// </summary>
        /// <param name="threadIdx"></param>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="nanite"></param>
        /// <returns>True if consumption happened</returns>
        public async ValueTask<bool> ConsumeAsync<T>(int threadIdx,
            Func<IoSink<TJob>, T, ValueTask> inlineCallback = null, T nanite = default)
        {
            try
            {
                //A job was produced. Dequeue it and process
                var curJob = await _queue.WaitAsync().FastPath();
                try
                {
                    if (Zeroed())
                        return false;

                    if (curJob.State != IoJobMeta.JobState.ProdConnReset)
                        await curJob.SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();

                    //Consume the job
                    if (await curJob.ConsumeAsync().FastPath() == IoJobMeta.JobState.Consumed ||
                        curJob.State is IoJobMeta.JobState.ConInlined or IoJobMeta.JobState.FastDup)
                    {
                        if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                        {
                            //forward any jobs                                                                             
                            await inlineCallback(curJob, nanite).FastPath();
                        }

                        //count the number of work done
                        IncEventCounter();
                    }
                    else if (curJob.State != IoJobMeta.JobState.RSync &&
                             curJob.State != IoJobMeta.JobState.ZeroRecovery &&
                             curJob.State != IoJobMeta.JobState.Fragmented &&
                             curJob.State != IoJobMeta.JobState.BadData && !Zeroed() && !curJob.Zeroed())
                    {
#if DEBUG
                            _logger?.Error($"consuming job: {curJob.Description} was unsuccessful, state = {curJob.State}");
                            curJob.PrintStateHistory();
#endif
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
                    try
                    {
                        if (!Zeroed())
                        {
                            //Consume success?
                            await curJob.SetStateAsync(curJob.State is IoJobMeta.JobState.Consumed
                                or IoJobMeta.JobState.Fragmented or IoJobMeta.JobState.BadData
                                ? IoJobMeta.JobState.Accept
                                : IoJobMeta.JobState.Reject).FastPath();

                            if (curJob.Id % parm_stats_mod_count == 0 && curJob.Id >= 9999)
                            {
                                await ZeroAtomicAsync(static (_, @this, _) =>
                                {
                                    @this.DumpStats();
                                    return new ValueTask<bool>(true);
                                }, this).FastPath();
                            }

                            await ZeroJobAsync(curJob, curJob.FinalState != IoJobMeta.JobState.Accept).FastPath();

                            //back pressure
                            Source.BackPressure(zeroAsync: true);
                        }
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
            catch (Exception) when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                await Task.Delay(500);
                _logger?.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }
            
            return true;
        }

        /// <summary>
        /// Prepares job for recovery by latching onto previous jobs
        /// </summary>
        /// <param name="curJob">The job to prepare</param>
        /// <returns></returns>
        public async ValueTask<bool> PrimeForRecoveryAsync(IoSink<TJob> curJob)
        {
            //Sync previous fragments into this job
            if (ZeroRecoveryEnabled)
            {
                _previousJobFragment.Modified = false;
                var cur = _previousJobFragment.Head;
                var c = _previousJobFragment.Count << 1;
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
                        var qid = cur.Qid;
                        if (cur.Value.Id == curJob.Id - 1)
                        {
                            curJob.PreviousJob = cur.Value;
                            await _previousJobFragment.RemoveAsync(cur, qid).FastPath();
                            return cur.Value.EnableRecoveryOneshot = true;
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

            return false;
        }

        public string DumpStats()
        {
            return $"{Description} {EventCount / (double)IoUnixToMs.ElapsedMsToSec((long)UpTime):0.0} j/s, [{JobHeap.ReferenceCount} / {JobHeap.CacheSize} / {JobHeap.ReferenceCount + JobHeap.CacheSize} / {JobHeap.AvailableCapacity} / {JobHeap.Capacity}]\n" + Source.PrintCounters();
        }

        /// <summary>
        /// Starts processing work queues
        /// </summary>
        public virtual async ValueTask BlockOnReplicateAsync()
        {
            try
            {
                var desc = Description;
#if DEBUG
                _logger.Debug($"{GetType().Name}: Assimulating {desc}");
#endif
                //Consumer
                var width = Source.ZeroConcurrencyLevel();
                //var width = 1;
                for (var i = 0; i < width; i++)
                    await ZeroAsync(static async state =>
                    {
                        var (@this, i) = state;
                        try
                        {
                            //While supposed to be working
                            while (!@this.Zeroed())
                            {
                                try
                                {
                                    await @this.ConsumeAsync<object>(i).FastPath();
                                }
                                catch when (@this.Zeroed()) { }
                                catch (Exception e) when (!@this.Zeroed())
                                {
                                    @this._logger.Error(e, $"Consumption failed {@this.Description}");

                                    break;
                                }
                            }
                        }
                        catch when (@this.Zeroed())
                        {
                        }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger.Error(e, $"Consumption failed! {@this.Description}");
                        }
                    }, (this, i), TaskCreationOptions.DenyChildAttach).FastPath(); //TODO tuningO tuning

                //Producer
                width = Source.PrefetchSize;
                for (var i = 0; i < width; i++)
                {
                    await ZeroAsync(static async @this =>
                    {
                        try
                        {
                            //While supposed to be working
                            while (!@this.Zeroed())
                            {
                                try
                                {
                                    await @this.ProduceAsync().FastPath();
                                }
                                catch (Exception e)
                                {
                                    @this._logger.Error(e, $"Production failed: {@this.Description}");
                                    break;
                                }
                            }
                        }
                        catch when (@this.Zeroed())
                        {
                        }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger.Error(e, $"Production failed! {@this.Description}");
                        }
                    }, this, TaskCreationOptions.DenyChildAttach).FastPath(); //TODO tuning
                }

                await AsyncTasks.Token.BlockOnNotCanceledAsync().FastPath();
#if DEBUG
                _logger?.Trace($"{GetType().Name}: Processing for {desc} stopped");
#endif
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger?.Error(e,Description);
            }
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
    }
}
