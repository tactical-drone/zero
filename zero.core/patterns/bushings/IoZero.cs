using System;
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
            bool enableZeroRecovery, bool cascadeOnSource = true, int concurrencyLevel = 0) : base($"{nameof(IoSink<TJob>)}", concurrencyLevel <= 0? source?.ZeroConcurrencyLevel?? 1 : concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();

            ZeroRecoveryEnabled = enableZeroRecovery;

            ConfigureAsync(description, source, mallocJob, cascadeOnSource).AsTask().GetAwaiter();

            //TODO tuning
            if (ZeroRecoveryEnabled)
                _previousJobFragment = new IoQueue<IoSink<TJob>>($"{description}", (Source.PrefetchSize + Source.ZeroConcurrencyLevel) * 3, Source.PrefetchSize);

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
            var capacity = Source.PrefetchSize;

            //These numbers were numerically established
            if (ZeroRecoveryEnabled)
                capacity *= 2;

            try
            {
                //TODO tuning
                IIoZeroSemaphoreBase<IoSink<TJob>> c = new IoZeroCore<IoSink<TJob>>(description, capacity, AsyncTasks);
                _queue = c.ZeroRef(ref c);
                
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
        private IIoZeroSemaphoreBase<IoSink<TJob>> _queue;

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
            try
            {
                await base.ZeroManagedAsync().FastPath();

                if(Source != null) //TODO: how can this be?
                    await Source.DisposeAsync(this, $"cascade; {ZeroReason}").FastPath();

                _queue?.ZeroSem();

                if (JobHeap != null)
                {
                    await JobHeap.ZeroManagedAsync(static async (sink, @this) =>
                    {
                        await sink.DisposeAsync(@this, $"cascade; {@this.ZeroReason}").FastPath();
                    }, this).FastPath();
                }
            
                if (_previousJobFragment != null)
                {
                    await _previousJobFragment.ZeroManagedAsync(static (sink, @this) => sink.Value.DisposeAsync(@this, $"teardown; {@this.Description}").FastPath(), this, zero: true).FastPath();
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, $"IoZero::ZeroManagedAsync: {Description}");
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
                        _logger.Warn($"({GetType().Name}<{typeof(TJob).Name}>) {nameof(_previousJobFragment)} has grown large {_previousJobFragment.Count}/{_previousJobFragment.Capacity} ");
                    }
#endif
                    var ts = Environment.TickCount;
                    //Produce job input
                    if (!Zeroed() && await nextJob.ProduceAsync(this).FastPath() == IoJobMeta.JobState.Produced || nextJob.State == IoJobMeta.JobState.ProdConnReset)
                    {
                        //If a job Id has not been assigned it's not ideal, but we do it here in case...
                        nextJob.GenerateJobId();

                        await Source.WaitForBackPressureAsync().FastPath();

                        if (ZeroRecoveryEnabled)
                        {
                            retry:
                            Interlocked.Exchange(ref nextJob.FragmentIdx, await _previousJobFragment.EnqueueAsync(nextJob).FastPath());

                            if (nextJob.FragmentIdx == null && _previousJobFragment.Count == _previousJobFragment.Capacity)
                            {
                                var flushedJob = await _previousJobFragment.DequeueAsync().FastPath();
                                _logger.Fatal($"{nameof(ProduceAsync)}: Flushing recovery tail index {flushedJob.Id}, {flushedJob.Description}");
                                await ZeroJobAsync(flushedJob, true);
                                goto retry;
                            }
                        }

                        //Enqueue the job for the consumer
                        if (nextJob.State != IoJobMeta.JobState.ProdConnReset)
                            await nextJob.SetStateAsync(IoJobMeta.JobState.Queued).FastPath();

                        if(_queue.Release(nextJob, true) < 0)
                        {
                            ts = ts.ElapsedMs();

                            await nextJob.SetStateAsync(IoJobMeta.JobState.QueuedError).FastPath();
                            await ZeroJobAsync(nextJob, true).FastPath();
                            nextJob = null;

                            //IsArbitrating = false;
                            //Is the producer spinning? Slow it down
                            int throttleTime;
                            if ((throttleTime = parm_min_failed_production_time - ts) > 0)
                                await Task.Delay(throttleTime, AsyncTasks.Token);

                            if(!Zeroed())
                                _logger.Warn($"Producer stalled.... {Description}");

                            return Source.BackPressure(zeroAsync: false) > 0; //maybe we retry instead of crashing the producer
                        }

                        //Pass control over to the consumer
                        nextJob = null;

                        if (!IsArbitrating)
                            IsArbitrating = true;

                        return true;
                    }
                    else //produce job returned with errors or nothing...
                    {
                        var accept = false;
                        if (nextJob.State == IoJobMeta.JobState.ProdSkipped)
                        {
                            await nextJob.SetStateAsync(IoJobMeta.JobState.Accept).FastPath();
                            accept = true;
                        }
                        else
                            IsArbitrating = false;

                        await ZeroJobAsync(nextJob, nextJob.FinalState != IoJobMeta.JobState.Accept).FastPath();
                        nextJob = null;

                        //Are we in teardown?
                        if (Zeroed())
                            return false;

                        return accept;
                    }
                }
                else
                {
                    //Are we in teardown?
                    if (Zeroed() || JobHeap.Zeroed)
                        return false;

                    _logger.Warn($"Production [FAILED]; {JobHeap.Description}");
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
                        if(job.EnableRecoveryOneshot)
                            job.ZeroRecovery.SetResult(true);
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e,Description);
                    }

                    if (purge)
                    {
                        if (job.PreviousJob != null)
                        {
                            JobHeap.Return((IoSink<TJob>)job.PreviousJob);
                            job.PreviousJob = null;
                        }

                        var latch = job.FragmentIdx;
                        var qid = job.FragmentIdx?.Qid ?? -1;
                        if (latch != null && Interlocked.CompareExchange(ref job.FragmentIdx, null, latch) == latch)
                        {
                            if(await _previousJobFragment.RemoveAsync(latch, qid).FastPath())
                                JobHeap.Return(job, true);
                        }
                    }
                    else
                    {
                        if (job.PreviousJob != null)
                        {
                            JobHeap.Return((IoSink<TJob>)job.PreviousJob);
                            job.PreviousJob = null;
                        }
                        else
                        {
                            //It is unlikely that an accepted job has future jobs depending on it
                            if (job.FinalState == IoJobMeta.JobState.Accept)
                            {
                                var latch = job.FragmentIdx;
                                var qid = job.FragmentIdx?.Qid ?? -1;
                                if (latch != null && Interlocked.CompareExchange(ref job.FragmentIdx, null, latch) ==
                                    latch)
                                {
                                    if(await _previousJobFragment.RemoveAsync(latch, qid).FastPath())
                                        JobHeap.Return(job);
                                }
                            }
                        }
                    }
                }
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e,$"s = {job?.FinalState}");
            }
        }

        /// <summary>
        /// Consume 
        /// </summary>
        /// <param name="consume">The inline callback.</param>
        /// <param name="context"></param>
        /// <returns>True if consumption happened</returns>
        public async ValueTask<bool> ConsumeAsync<T>(Func<IoSink<TJob>, T, ValueTask> consume = null, T context = default)
        {
            try
            {
                //A job was produced. Dequeue it and process
                var curJob = await _queue.WaitAsync().FastPath();

                if (curJob.State != IoJobMeta.JobState.ProdConnReset)
                    await curJob.SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();

                //Consume the job
                //IoZeroScheduler.Zero.LoadAsyncContext(static async state => { 
                    var @this = this;
                        //var (@this, curJob, consume, context) = (ValueTuple<IoZero<TJob>, IoSink<TJob>, Func<IoSink<TJob>, T, ValueTask>, T>)state;
                        try
                        {
                        if (@this.Zeroed())
                            return false;
                            //return;

                            if (await curJob.ConsumeAsync().FastPath() == IoJobMeta.JobState.Consumed ||
                                curJob.State is IoJobMeta.JobState.ConInlined or IoJobMeta.JobState.FastDup)
                            {
                                if (curJob.State == IoJobMeta.JobState.ConInlined && consume != null)
                                    await consume(curJob, context).FastPath();

                                //count the number of work done
                                @this.IncEventCounter();
                            }
                            else if (curJob.State != IoJobMeta.JobState.RSync &&
                                     curJob.State != IoJobMeta.JobState.ZeroRecovery &&
                                     curJob.State != IoJobMeta.JobState.Fragmented &&
                                     curJob.State != IoJobMeta.JobState.BadData && !@this.Zeroed() && !curJob.Zeroed())
                            {
    #if DEBUG
                                @this._logger.Error($"consuming job: {curJob.Description} was unsuccessful, state = {curJob.State}");
                                curJob.PrintStateHistory("stale");
    #endif
                            }
                        }
                        catch (Exception) when (@this.Zeroed() || curJob.Zeroed())
                        {
                        }
                        catch (Exception e) when (!@this.Zeroed() && !curJob.Zeroed())
                        {
                            @this._logger.Error(e,$"{@this.Description}: {curJob.TraceDescription} consuming job: {curJob.Description} returned with errors:");
                        }
                        finally
                        {
                            try
                            {
                                if (!@this.Zeroed())
                                {
                                    if (curJob.State is IoJobMeta.JobState.Fragmented or IoJobMeta.JobState.BadData)
                                    {
                                        await curJob.SetStateAsync(IoJobMeta.JobState.Recovering).FastPath();
                                    }
                                    else
                                    {
                                        //Consume success?
                                        await curJob.SetStateAsync(curJob.State is IoJobMeta.JobState.Consumed
                                            ? IoJobMeta.JobState.Accept
                                            : IoJobMeta.JobState.Reject).FastPath();
                                    }

                                    //log stats to console
                                    if (curJob.Id % @this.parm_stats_mod_count == 0 && curJob.Id >= 9999)
                                    {
                                        await @this.ZeroAtomicAsync(static (_, @this, _) =>
                                        {
                                            @this.DumpStats();
                                            return new ValueTask<bool>(true);
                                        }, @this).FastPath();
                                    }
                                }
                            }
                            catch when (@this.Zeroed())
                            {
                            }
                            catch (Exception e) when (!@this.Zeroed())
                            {
                                @this._logger.Fatal(e, $"{nameof(ConsumeAsync)}:");
                            }
                            finally
                            {
                                //cleanup
                                await @this.ZeroJobAsync(curJob, curJob.FinalState is IoJobMeta.JobState.Reject).FastPath();
                                //back pressure
                                @this.Source.BackPressure(zeroAsync: true);
                            }
                        }
                //}, (this, curJob, consume, context));
                return true;
            }
            catch (Exception) when (Zeroed()) {}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }
            
            return false;
        }

        /// <summary>
        /// Prepares job for recovery by latching onto previous jobs
        /// </summary>
        /// <param name="curJob">The job to prepare</param>
        /// <returns>True if primed, false otherwise</returns>
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
                            if (await _previousJobFragment.RemoveAsync(cur, qid).FastPath())
                            {
                                curJob.PreviousJob = cur.Value;
                                cur.Value.EnableRecoveryOneshot = true;
                                Interlocked.MemoryBarrier();
                                return cur.Value.EnableRecoveryOneshot;
                            }
                            return false;
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
            try
            {
                return $"{Description} {EventCount / (double)UpTime.ElapsedUtcMsToSec():0.0} j/s, [{JobHeap.ReferenceCount} / {JobHeap.CacheSize} / {JobHeap.ReferenceCount + JobHeap.CacheSize} / {JobHeap.AvailableCapacity} / {JobHeap.Capacity}]\n" + Source.PrintCounters();
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(DumpStats)}:");
                return e.Message;
            }

            return string.Empty;
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
                var width = Source.ZeroConcurrencyLevel;
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
                                    await @this.ConsumeAsync<object>().FastPath();
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
                    }, (this, i)).FastPath(); //TODO tuningO tuning

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
                                    if(!await @this.ProduceAsync().FastPath())
                                        await Task.Delay(@this.parm_min_failed_production_time, @this.AsyncTasks.Token);
                                }
                                catch when (@this.Zeroed()) { }
                                catch (Exception e) when (!@this.Zeroed())
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
                    }, this).FastPath(); //TODO tuning
                }

                await AsyncTasks.BlockOnNotCanceledAsync().FastPath();
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
