using System;
using System.Collections.Generic;
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
using zero.core.patterns.semaphore.core;

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
            Source = source;
            var capacity = Source.PrefetchSize + Source.ZeroConcurrencyLevel();

            //These numbers were numerically established
            if (ZeroRecoveryEnabled)
                capacity += Source.ZeroConcurrencyLevel() * 4;

            try
            {
                //TODO tuning
                _queue = new IoZeroQ<IoSink<TJob>>($"zero Q: {_description}", capacity, asyncTasks:AsyncTasks, concurrencyLevel:ZeroConcurrencyLevel(),zeroAsyncMode:false);
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

            //TODO tuning
            if (ZeroRecoveryEnabled)
                Volatile.Write(ref _previousJobFragment, new IoQueue<IoSink<TJob>>($"{description}", capacity, Source.PrefetchSize));
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
        //private IoQueue<IoSink<TJob>> _queue;
        private IoZeroQ<IoSink<TJob>> _queue;

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
        private IoQueue<IoSink<TJob>> _previousJobFragment;

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
        public int parm_min_failed_production_time = 2500;

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
            await base.ZeroManagedAsync().FastPath();

            await Source.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

            await _queue.ZeroManagedAsync(static async (sink, @this) => await sink.DisposeAsync(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath(), this,zero: true).FastPath();

            await JobHeap.ZeroManagedAsync(static async (sink, @this) =>
            {
                await sink.DisposeAsync(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            },this).FastPath();

            if (_previousJobFragment != null)
            {
                await _previousJobFragment.ZeroManagedAsync(static (sink, @this) => sink.Value.DisposeAsync(@this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath(), this, zero: true).FastPath();
            }

#if DEBUG
            _logger.Trace($"Closed {Description} from {ZeroedFrom}: reason = {ZeroReason}");
#endif
        }

        //private List<IoSink<TJob>> _backlog = new List<IoSink<TJob>>();

        /// <summary>
        /// Produce
        /// </summary>
        /// <returns></returns>
        public async ValueTask<bool> ProduceAsync() 
        {
            IoSink<TJob> nextJob = null;
            try
            {
                try
                {
                    //wait for prefetch pressure
                    if (!await Source.WaitForPrefetchPressureAsync().FastPath())
                        return false;

                    //Allocate a job from the heap
                    nextJob = await JobHeap.TakeAsync(null, this).FastPath();
                    //_backlog.Add(nextJob);

#if DEBUG
                    if (ZeroRecoveryEnabled)
                        Debug.Assert(JobHeap.ReferenceCount - 1 <= Source.PrefetchSize + Source.ZeroConcurrencyLevel() << 1);
                    else
                        Debug.Assert(JobHeap.ReferenceCount <= Source.PrefetchSize + Source.ZeroConcurrencyLevel());
#endif


                    if (nextJob != null)
                    {
                        await nextJob.SetStateAsync(IoJobMeta.JobState.Producing).FastPath();

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
                        if (!Zeroed() && await nextJob.ProduceAsync(static async (job, @this) =>
                            {
                                //Block on producer back pressure
                                if (!await job.Source.WaitForBackPressureAsync().FastPath())
                                    return false;

                                return true;
                            }, this).FastPath() == IoJobMeta.JobState.Produced)
                        {
#if DEBUG
                            //_logger.Debug($"{nameof(ProduceAsync)}: id = {nextJob.Id}, #{nextJob.Serial} - {Description}");

                            if (_queue.Count > 1 && _queue.Count > _queue.Capacity * 2 / 3)
                                _logger.Warn($"[[ENQUEUE]] backlog = {_queue.Count}/{_queue.Capacity}, {nextJob.Description}, {Description}");
#endif
                            if (ZeroRecoveryEnabled)
                            {
                                if ((nextJob.FragmentIdx = await _previousJobFragment.EnqueueAsync(nextJob).FastPath()) == null &&
                                    _previousJobFragment.Count == _previousJobFragment.Capacity)
                                {
                                    _logger.Warn(
                                        $"Flushing previous job cash {_previousJobFragment.Count} items, {Description}");
                                    await _previousJobFragment.ClearAsync().FastPath();
                                }
                                else if(nextJob.FragmentIdx == null)
                                {
                                    
                                }
                            }

                            //Enqueue the job for the consumer
                            await nextJob.SetStateAsync(IoJobMeta.JobState.Queued).FastPath();

                            if (_queue.TryEnqueue(nextJob, false,static nextJob =>
                                {
                                    if (nextJob.Id == -1)
                                        nextJob.GenerateJobId();
                                },(nextJob)) < 0 || nextJob.Source == null)
                            {
                                ts = ts.ElapsedMs();

                                await nextJob.SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();
                                await ZeroJobAsync(nextJob, true).FastPath();
                                nextJob = null;

                                if (Source.BackPressure() != 1)
                                {
                                    _logger.Fatal($"{nameof(ConsumeAsync)}: Backpressure [FAILED] - {Description}");
                                }

                                if (Source.PrefetchPressure(zeroAsync: false) != 1)
                                {
                                    _logger.Fatal($"{nameof(ConsumeAsync)}: PrefetchPressure [FAILED], {Description}");
                                }
                                
                                //how long did this failure take?

                                IsArbitrating = false;
                                //Is the producer spinning? Slow it down
                                int throttleTime;
                                if ((throttleTime = parm_min_failed_production_time - ts) > 0)
                                    await Task.Delay(throttleTime, AsyncTasks.Token);

                                _logger.Warn($"Producer stalled.... {Description}");
                                return true;  //maybe we retry instead of crashing the producer
                            }

                            //Pass control over to the consumer
                            nextJob = null;

                            //Signal to the consumer that there is work to do
                            //Source.Pressure();

                            //Fetch more work
                            if (Source.PrefetchPressure(zeroAsync: false) != 1)
                            {
                                _logger.Fatal($"{nameof(ConsumeAsync)}: PrefetchPressure [FAILED] - {Description}");
                            }

                            if (!IsArbitrating)
                                IsArbitrating = true;

                            //TODO:Is this still a good idea?
                            if (ZeroRecoveryEnabled && _previousJobFragment.Count > Source.PrefetchSize + Source.ZeroConcurrencyLevel() * 2)
                            {
                                var drop = await _previousJobFragment.DequeueAsync().FastPath();
                                JobHeap.Return(drop, drop.EnableRecoveryOneshot || drop.FinalState != 0, true);
                            }

                            return true;
                        }
                        else //produce job returned with errors or nothing...
                        {
                            //how long did this failure take?
                            ts = ts.ElapsedMs();
                            IsArbitrating = false;

                            if(nextJob.State == IoJobMeta.JobState.ProdSkipped)
                                await nextJob.SetStateAsync(IoJobMeta.JobState.Accept).FastPath();

                            await ZeroJobAsync(nextJob, nextJob.State != IoJobMeta.JobState.Accept).FastPath();
                            nextJob = null;

                            //signal back pressure
                            if (Source.BackPressure() != 1)
                            {
                                _logger.Fatal($"{nameof(ConsumeAsync)}: BackPressure [FAILED] - {Description}");
                            }

                            // prefetch pressure
                            if (Source.PrefetchPressure() != 1)
                            {
                                _logger.Fatal($"{nameof(ConsumeAsync)}: PrefetchPressure [FAILED] - {Description}");
                            }
                            

                            //Are we in teardown?
                            if (Zeroed())
                                return false;

                            //Is the producer spinning? Slow it down
                            int throttleTime;
                            if((throttleTime = parm_min_failed_production_time - ts) > 0)
                                await Task.Delay(throttleTime, AsyncTasks.Token);
                            
                            return false;
                        }
                    }
                    else
                    {
                        //Are we in teardown?
                        if (Zeroed() || JobHeap.Zeroed)
                            return false;

                        if (Source.BackPressure() != 1)
                        {
                            _logger.Fatal($"{nameof(ConsumeAsync)}: BackPressure [FAILED] - {Description}");
                        }

                        // prefetch pressure
                        if (Source.PrefetchPressure() != 1)
                        {
                            _logger.Fatal($"{nameof(ConsumeAsync)}: PrefetchPressure [FAILED] - {Description}");
                        }

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

                        await ZeroJobAsync(nextJob, true).FastPath();
                        nextJob = null;
                    }
                }
            }
            catch (Exception) when (Zeroed() || nextJob != null && nextJob.Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed() && nextJob != null)
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
        private async ValueTask ZeroJobAsync(IoSink<TJob> job, bool purge = false)
        {
            IoSink<TJob> prevJob = null;
            try
            {
                if (job == null)
                    return;

                if (purge && job.FinalState == 0)
                {
#if DEBUG
                    _logger.Fatal($"sink purged -> {job.Description}, {Description}");
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
                        job.ZeroRecovery.SetResult(job.ZeroEnsureRecovery());
                    }
                    catch
                    {
                        // ignored
                    }

                    if ((prevJob = (IoSink<TJob>)job.PreviousJob) != null)
                    {
                        Debug.Assert(prevJob.PreviousJob == null);
                        //is the job complete?
                        job.PreviousJob = null;
                        JobHeap.Return(prevJob);
                    }

                    if (purge)
                    {
                        var latch = job.FragmentIdx;
                        if (latch != null && Interlocked.CompareExchange(ref job.FragmentIdx, null, latch) == latch)
                            await _previousJobFragment.RemoveAsync(latch).FastPath();

                        //_logger.Error($"{nameof(ZeroJobAsync)}: id = {job.Id}, #{job.Serial}");
                        JobHeap.Return(job, true, true);
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
        /// <param name="threadIndex"></param>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="nanite"></param>
        /// <returns>True if consumption happened</returns>
        public async ValueTask<bool> ConsumeAsync<T>(int threadIndex, Func<IoSink<TJob>, T, ValueTask> inlineCallback = null, T nanite = default)
        {
            
            //Wait for producer pressure
            //if (!await Source.WaitForPressureAsync().FastPath())
            //    return false;

            try
            {
                //A job was produced. Dequeue it and process
                await foreach (var curJob in _queue.BalanceOnConsumeAsync(threadIndex))
                {
                    if (curJob == null)
                        return false;
                    Debug.Assert(curJob != null);
#if DEBUG
                    if (_queue.Count > 1 && _queue.Count > _queue.Capacity * 2 / 3)
                        _logger.Warn($"[[DEQUEUE]] backlog = {_queue.Count}, {curJob.Description}, {Description}");
#endif
                    try
                    {
                        await curJob.SetStateAsync(IoJobMeta.JobState.Consuming).FastPath();

                        //await PrimeForRecoveryAsync(curJob).FastPath();

                        //Consume the job
                        if (await curJob.ConsumeAsync().FastPath() == IoJobMeta.JobState.Consumed || curJob.State is IoJobMeta.JobState.ConInlined or IoJobMeta.JobState.FastDup)
                        {
                            //if (inlineCallback != null)
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
                        try
                        {
                            //Consume success?
                            await curJob.SetStateAsync(curJob.State is IoJobMeta.JobState.Consumed ? IoJobMeta.JobState.Accept : IoJobMeta.JobState.Reject).FastPath();

                            if (curJob.Id % parm_stats_mod_count == 0 && curJob.Id >= 9999)
                            {
                                await ZeroAtomicAsync(static (_, @this, _) =>
                                {
                                    @this.DumpStats();
                                    return new ValueTask<bool>(true);
                                }, this).FastPath();
                            }

                            await ZeroJobAsync(curJob, curJob.FinalState == IoJobMeta.JobState.Accept).FastPath();

                            if (Source.BackPressure(zeroAsync: false) != 1)//TODO: Why is true here cata?
                            {
                                _logger.Fatal($"{nameof(ConsumeAsync)}: Backpressure [FAILED], {Description}");
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

                    //return true;
                }

                //if (curJob == null) return false;

                //await ZeroJobAsync(curJob, true).FastPath();
                //curJob = null;
                

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
            //finally
            //{
            //    //if (curJob != null)
            //        //throw new ApplicationException($"{curJob} not freed!!!");
            //}

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
                        if (cur.Value.Id == curJob.Id - 1)
                        {
                            curJob.PreviousJob = cur.Value;
                            await _previousJobFragment.RemoveAsync(cur).FastPath();
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
                _logger.Trace($"{GetType().Name}: Assimulating {desc}");
#endif
                //Consumer
                var width = Source.ZeroConcurrencyLevel();
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
                    }, (this,i), TaskCreationOptions.DenyChildAttach).FastPath(); //TODO tuning

                //Producer
                width = Source.PrefetchSize;
                for (var i = 0; i < width; i++)
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
