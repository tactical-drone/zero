﻿using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
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
        /// <param name="concurrencyLevel"></param>
        protected IoZero(string description, IoSource<TJob> source, Func<object, IIoNanite, IoSink<TJob>> mallocJob,
            bool enableSync, bool sourceZeroCascade = false, int concurrencyLevel = 1) : base($"{nameof(IoSink<TJob>)}", concurrencyLevel < 0? source.ZeroConcurrencyLevel() : concurrencyLevel)
        {
            ConfigureAsync(description, source, mallocJob, sourceZeroCascade).AsTask().GetAwaiter();

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

            //TODO tuning
            if (SyncRecoveryModeEnabled)
                _previousJobFragment = new IoQueue<IoSink<TJob>>($"{description}", 5, ZeroConcurrencyLevel());
        }

        /// <summary>
        /// Configures the source.
        /// </summary>
        /// <param name="description">A description of the source</param>
        /// <param name="source">An instance of the source</param>
        /// <param name="mallocMessage"></param>
        /// <param name="sourceZeroCascade"></param>
        private async ValueTask ConfigureAsync(string description, IoSource<TJob> source,
            Func<object, IIoNanite, IoSink<TJob>> mallocMessage, bool sourceZeroCascade = false)
        {
            _description = description;
            
            JobHeap = new IoHeapIo<IoSink<TJob>>($"{nameof(JobHeap)}: {_description}",parm_max_q_size, true) { Make = mallocMessage, Context = source};

            Source = source ?? throw new ArgumentNullException($"{nameof(source)}");
            await Source.ZeroHiveAsync(this, sourceZeroCascade).FastPath().ConfigureAwait(Zc);

            //TODO tuning
            _queue = new IoQueue<IoSink<TJob>>($"zero Q: {_description}", (uint)(ZeroConcurrencyLevel() * 2 + Source.PrefetchSize), ZeroConcurrencyLevel());
        }

        /// <summary>
        /// The source of the work
        /// </summary>
        public volatile IoSource<TJob> Source;

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

        /// <summary>
        /// Maximum amount of producers that can be buffered before we stop production of new jobs
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public uint parm_max_q_size = 256; //TODO

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
            await _queue.ZeroManagedAsync<object>(zero:true).FastPath().ConfigureAwait(Zc);

            if(_previousJobFragment != null)
                await _previousJobFragment.ZeroManagedAsync<object>(zero:true).FastPath().ConfigureAwait(Zc);

            await JobHeap.ZeroManagedAsync(static async (sink, @this) =>
            {
                await sink.ZeroAsync(@this).FastPath().ConfigureAwait(@this.Zc);
            },this).FastPath().ConfigureAwait(Zc);

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

#if DEBUG
            _logger.Trace($"Closed {Description}, from :{ZeroedFrom?.Description}");
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
                    nextJob = await JobHeap.TakeAsync(constructor: static (load, ioZero) =>
                    {
                        load.IoZero = ioZero;
                        return new ValueTask<IoSink<TJob>>(load);
                    }, this).FastPath().ConfigureAwait(Zc);

                    //Allocate a job from the heap
                    if (nextJob != null)
                    {
                        nextJob.State = IoJobMeta.JobState.Producing;

                        //wait for prefetch pressure
                        if (nextJob.Source.PrefetchEnabled && enablePrefetchOption)
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
                            if (SyncRecoveryModeEnabled && (prevJobFragment = await _previousJobFragment.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
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
                                    if (!await job.Source.WaitForBackPressureAsync().FastPath().ConfigureAwait(@this.Zc))
                                    {
                                        job.State = IoJobMeta.JobState.ProduceErr;
                                        return false;
                                    }
                                }
                                catch (Exception) when (@this.Zeroed()) { }
                                catch (Exception e) when (!@this.Zeroed())
                                {
                                    @this._logger.Error(e, $"Producer barrier failed for {@this.Description}");
                                    job.State = IoJobMeta.JobState.ProduceErr;
                                    return false;
                                }

                                return true;
                            }, this).FastPath().ConfigureAwait(Zc) == IoJobMeta.JobState.Produced && !Zeroed())
                            {
                                _producerStopwatch.Stop();

                                if (SyncRecoveryModeEnabled)
                                    await _previousJobFragment.EnqueueAsync(nextJob).FastPath()
                                        .ConfigureAwait(Zc);

                                //signal back pressure
                                if (nextJob.Source.PrefetchEnabled && enablePrefetchOption)
                                    await nextJob.Source.PrefetchPressure().FastPath().ConfigureAwait(Zc);

                                //Enqueue the job for the consumer
                                nextJob.State = IoJobMeta.JobState.Queued;

                                if (await _queue.EnqueueAsync(nextJob).FastPath().ConfigureAwait(Zc) == null)
                                    return false;

                                //Pass control over to the consumer
                                nextJob = null;

                                //Signal to the consumer that there is work to do
                                return await Source.PressureAsync().FastPath().ConfigureAwait(Zc) > 0;
                            }
                            else //produce job returned with errors or nothing...
                            {
                                //how long did this failure take?
                                _producerStopwatch.Stop();
                                IsArbitrating = false;

                                // Signal prefetch back pressure
                                if (enablePrefetchOption && nextJob.Source is { PrefetchEnabled: true })
                                    await nextJob.Source.PrefetchPressure().FastPath().ConfigureAwait(Zc);

                                //ReturnJobToHeapAsync job
                                nextJob.State = IoJobMeta.JobState.Reject;
                                await ReturnJobToHeapAsync(nextJob, true).FastPath()
                                    .ConfigureAwait(Zc);

                                //Are we in teardown?
                                if (Zeroed() || nextJob.Zeroed() || Source.Zeroed())
                                    return false;

                                //Is there a bug in the producer?
                                if (nextJob.State == IoJobMeta.JobState.Producing)
                                {
                                    _logger.Warn($"{nameof(ProduceAsync)} - {Description}: ({nextJob.Description}): State remained {IoJobMeta.JobState.Producing}");
                                    nextJob.State = IoJobMeta.JobState.Error;
                                }

                                //Is the producer spinning? Slow it down
                                if (_producerStopwatch.ElapsedMilliseconds < parm_min_failed_production_time)
                                    await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token).ConfigureAwait(Zc);

                                //signal back pressure
                                await Source.BackPressureAsync().FastPath().ConfigureAwait(Zc);
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
                    _logger?.Error(e, $"{GetType().Name}: Producing `{Description}' returned with errors:");
                    return false;
                }
                finally
                {
                    //prevent leaks
                    if (nextJob != null)
                    {
                        if (!Zeroed() && !nextJob.Zeroed() && !Source.Zeroed())
                            _logger.Error(
                                $"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed..., state = {nextJob.State}");

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
            catch (Exception) when (Zeroed() || Source.Zeroed()) {}
            catch (Exception e) when (!Zeroed() && !Source.Zeroed())
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
                Debug.Assert(job!= null);

#if RELEASE
                if (job == null)
                    return;
#endif

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
                        await JobHeap.ReturnAsync((IoSink<TJob>)job.PreviousJob, job.PreviousJob.FinalState != IoJobMeta.JobState.Accept).FastPath().ConfigureAwait(Zc);
                    else
                        await _previousJobFragment.PushAsync((IoSink<TJob>) job.PreviousJob).FastPath().ConfigureAwait(Zc);

                    job.PreviousJob = null;
                    return;
                }
                //else if (!parent && job.CurrentState.Previous.CurrentState == IoJobMeta.CurrentState.Accept)
                //{
                //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
                //}

                if (freeCurrent || !SyncRecoveryModeEnabled)
                    await JobHeap.ReturnAsync(job, job.FinalState != IoJobMeta.JobState.Accept).FastPath().ConfigureAwait(Zc); ;
                
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
            if (Zeroed() || Source.Zeroed())
                return false;

            try
            {
                //Wait for producer pressure
                if (!await Source.WaitForPressureAsync().FastPath().ConfigureAwait(Zc))
                {
                    //Was shutdown requested?
                    if (Zeroed() || Source.Zeroed()) 
                        return false;
                    
                    //wait...
                    _logger.Trace($"{Description}: {nameof(Source.WaitForPressureAsync)} timed out waiting, willing to wait {parm_consumer_wait_for_producer_timeout}ms");
                    await Task.Delay(parm_consumer_wait_for_producer_timeout / 4, AsyncTasks.Token).ConfigureAwait(Zc);

                    //Try again
                    return false;
                }
                
                //A job was produced. Dequeue it and process
                var curJob = await _queue.DequeueAsync().FastPath().ConfigureAwait(Zc);
                if (curJob != null)
                {
                    curJob.State = IoJobMeta.JobState.Consuming;
                    try
                    {

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
                            }

                            curJob.State = IoJobMeta.JobState.Consumed;

                            await Source.BackPressureAsync().FastPath().ConfigureAwait(Zc);

                            //sync previous failed job buffers
                            if (SyncRecoveryModeEnabled)
                                curJob.JobSync();
                        }
                        else if (!Zeroed() && !curJob.Zeroed() && !Source.Zeroed())
                        {
                            _logger.Error($"{Description}: {curJob.TraceDescription} consuming job: {curJob.Description} was unsuccessful, state = {curJob.State}");
                            curJob.State = IoJobMeta.JobState.Error;
                        }
                    }
                    catch (Exception) when (Zeroed()) {}
                    catch (Exception e) when (!Zeroed())
                    {
                        _logger?.Error(e,$"{Description}: {curJob.TraceDescription} consuming job: {curJob.Description} returned with errors:");
                    }
                    finally
                    {
                        //Consume success?
                        curJob.State = curJob.State < IoJobMeta.JobState.Error
                            ? IoJobMeta.JobState.Accept
                            : IoJobMeta.JobState.Reject;

                        try
                        {

                            if (curJob.Id % parm_stats_mod_count == 0 && _lastStat.ElapsedDelta() > 10)
                            {
                                lock (Environment.Version)
                                {
                                    _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                    _logger?.Info($"{Description} {JobHeap.OpsPerSecond:0.0} j/s, [{JobHeap.ReferenceCount} / {JobHeap.CacheSize()} / {JobHeap.ReferenceCount + JobHeap.CacheSize()} / {JobHeap.FreeCapacity()} / {JobHeap.MaxSize}]");
                                    curJob.Source.PrintCounters();
                                }
                            }

                            await ReturnJobToHeapAsync(curJob).FastPath().ConfigureAwait(Zc);
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
            catch (Exception) when(Zeroed()){}
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
#if DEBUG
            _logger.Trace($"{GetType().Name}: Assimulating {Description}");
#endif

            //Producer
            _producerTask = ZeroOptionAsync(static async @this =>
            {
                try
                {
                    var produceTask = new ValueTask<bool>[@this.ZeroConcurrencyLevel()];
                    //While supposed to be working
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
                                break;
                            }
                        }

                        if (!await produceTask[^1].FastPath())
                            break;
                    }
                }
                catch when(@this.Zeroed()){}
                catch (Exception e) when(!@this.Zeroed())
                {
                    @this._logger.Error(e, $"Production failed! {@this.Description}");
                }
            },this, TaskCreationOptions.AttachedToParent | TaskCreationOptions.DenyChildAttach); //TODO tuning

            //Consumer
            _consumerTask = ZeroOptionAsync(static async @this =>
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
                        catch when (@this.Zeroed()) { }
                        catch (Exception e) when (!@this.Zeroed())
                        {
                            @this._logger.Error(e, $"Consumption failed {@this.Description}");
                            break;
                        }
                    }

                    if (!await consumeTaskPool[^1].FastPath())
                        break;
                }
            }, this, TaskCreationOptions.AttachedToParent | TaskCreationOptions.PreferFairness); //TODO tuning

            //Wait for tear down                
            await Task.WhenAll(_producerTask.AsTask(), _consumerTask.AsTask()).ConfigureAwait(Zc);

#if DEBUG
            _logger.Trace($"{GetType().Name}: Processing for {Description} stopped");
#endif
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
