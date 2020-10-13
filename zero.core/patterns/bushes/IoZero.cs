using System;
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
        protected IoZero(string description, IoSource<TJob> source, Func<object, IoSink<TJob>> mallocJob, bool sourceZeroCascade = false, int producers = 1, int consumers = 1)
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
            Func<object, IoSink<TJob>> mallocMessage, bool sourceZeroCascade = false)
        {
            _description = description;
            
            JobHeap = new IoHeapIo<IoSink<TJob>>(parm_max_q_size) { Make = mallocMessage };

            Source = source;

            ZeroOnCascade(Source, sourceZeroCascade);
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
        private ConcurrentQueue<IoSink<TJob>> _queue = new ConcurrentQueue<IoSink<TJob>>();
        
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
        private readonly ILogger _logger;

        /// <summary>
        /// Indicates whether jobs are being processed
        /// </summary>
        public bool IsArbitrating { get; set; }

        /// <summary>
        /// Maintains a handle to a job if fragmentation was detected so that the
        /// source can marshal fragments into the next production
        /// </summary>
        private volatile ConcurrentQueue<IoSink<TJob>> _previousJobFragment = new ConcurrentQueue<IoSink<TJob>>();

        /// <summary>
        /// Maximum amount of producers that can be buffered before we stop production of new jobs
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_max_q_size = 500; //TODO

        /// <summary>
        /// Time to wait for insert before complaining about it
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_ingress_timout = 5000;

        /// <summary>
        /// Debug output rate
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
#if DEBUG
        public int parm_stats_mod_count = 5000;
#else
        public int parm_stats_mod_count = 5000;
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
            JobHeap.ZeroManaged();

            _queue.ToList().ForEach(q => q.ZeroAsync(this).ConfigureAwait(false));
            _queue.Clear();

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
                    IoSink<TJob> nextJob = null;
                    try
                    {
                        nextJob = await JobHeap.TakeAsync(parms: (load, closure) =>
                        {
                            load.IoZero = (IIoZero) closure;
                            return new ValueTask<IoSink<TJob>>(load);
                        }, this).ZeroBoostAsync().ConfigureAwait(false);

                        
                        //Allocate a job from the heap
                        if (!Zeroed() && nextJob != null) //TODO 
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
                                }, this).ZeroBoostAsync().ConfigureAwait(false) == IoJobMeta.JobState.Produced && !Zeroed())
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
                                        nextJob = Free(nextJob, true);
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
                                        nextJob = Free(nextJob, true);
                                        IsArbitrating = false;

                                        //TEARDOWN
                                        await ZeroAsync(this).ConfigureAwait(false);
                                        return false;
                                    }
                                    
                                    //Is the producer spinning?
                                    if (_producerStopwatch.ElapsedMilliseconds < parm_min_failed_production_time)
                                        await Task.Delay(parm_min_failed_production_time, AsyncTasks.Token).ConfigureAwait(false);

                                    //Free job
                                    nextJob.State = IoJobMeta.JobState.Reject;
                                    nextJob = Free(nextJob, true);

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
                            if (nextJob != null)
                            {
                                nextJob.State = IoJobMeta.JobState.ProdCancel;
                                nextJob = Free(nextJob, true);
                            }
                            
                            //Are we in teardown?
                            if (Zeroed())
                            {
                                return false;
                            }

                            _logger.Warn($"{GetType().Name}: Production for: `{Description}` failed. Cannot allocate job resources!, heap =>  {JobHeap.CurrentHeapSize}/{JobHeap.MaxSize}");
                            await Task.Delay(parm_error_timeout, AsyncTasks.Token).ConfigureAwait(false);
                            return false;
                        }
                    }
                    catch (NullReferenceException) { nextJob = Free(nextJob, true); }
                    catch (ObjectDisposedException) { nextJob = Free(nextJob, true); }
                    catch (TimeoutException) { nextJob = Free(nextJob, true); }
                    catch (TaskCanceledException) { nextJob = Free(nextJob, true); }
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
                                _logger.Error($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job resources were not freed..., state = {nextJob.State}");

                            //TODO Double check this hack
                            if (nextJob.State != IoJobMeta.JobState.Finished)
                            {
                                //_logger.Fatal($"{GetType().Name} ({nextJob.GetType().Name}): [FATAL] Job status should be {nameof(IoJobMeta.CurrentState.Finished)}");
                                nextJob.State = IoJobMeta.JobState.Reject;
                            }
                            
                            nextJob = Free(nextJob, true);
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

        private IoSink<TJob> Free(IoSink<TJob> job, bool parent = false)
        {
            try
            {
                if (job == null)
                    return null;

                if (SupportsSync && job.PreviousJob != null)
                {
#if DEBUG
                    if (((IoSink<TJob>)job.PreviousJob).State != IoJobMeta.JobState.Finished)
                    {
                        _logger.Warn($"{GetType().Name}: PreviousJob fragment state = {((IoSink<TJob>)job.PreviousJob).State}");
                    }
#endif
                    JobHeap.Return((IoSink<TJob>)job.PreviousJob);
                    job.PreviousJob = null;
                    return null;
                }
                //else if (!parent && job.CurrentState.Previous.CurrentState == IoJobMeta.CurrentState.Accept)
                //{
                //    _logger.Fatal($"{GetHashCode()}:{job.GetHashCode()}: {job.Id}<<");
                //}

                if(parent || !SupportsSync)
                    JobHeap.Return(job);

            }
            catch (NullReferenceException e)
            {
                _logger.Trace(e,Description);
            }

            return null;
        }

        private long _lastStat = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        public ValueTask<bool> ConsumeAsync()
        {
            return ConsumeAsync(null, null);
        }

        /// <summary>
        /// Consume 
        /// </summary>
        /// <param name="inlineCallback">The inline callback.</param>
        /// <param name="zeroClosure"></param>
        /// <returns>True if consumption happened</returns>
        public async ValueTask<bool> ConsumeAsync(Func<IoSink<TJob>, IIoZero, Task> inlineCallback = null, IIoZero zeroClosure = null)
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
                        if (Zeroed() || AsyncTasks.IsCancellationRequested)
                        {
                            _logger.Trace($"{GetType().Name}: Consumer `{Description}' is shutting down");
                            return false;
                        }

                        //wait...
                        _logger.Trace($"{GetType().Name}: Consumer `{Description}' [[ProducerPressure]] timed out waiting on `{Description}', willing to wait `{parm_consumer_wait_for_producer_timeout}ms'");
                        await Task.Delay(parm_consumer_wait_for_producer_timeout / 4, AsyncTasks.Token).ConfigureAwait(false);

                        //Try again
                        return false;    
                    }
                }

                //A job was produced. Dequeue it and process
                if (!Zeroed() && _queue.TryDequeue(out var curJob))
                {
                    curJob.State = IoJobMeta.JobState.Consuming;
                    try
                    {
                        //Consume the job
                        if (await curJob.ConsumeAsync().ZeroBoostAsync().ConfigureAwait(false) == IoJobMeta.JobState.Consumed ||
                            curJob.State == IoJobMeta.JobState.ConInlined &&
                            !Zeroed())
                        {
                            if (curJob.State == IoJobMeta.JobState.ConInlined && inlineCallback != null)
                            {
                                //forward any jobs                                                                             
                                await inlineCallback(curJob, zeroClosure).ConfigureAwait(false);
                                curJob.State = IoJobMeta.JobState.Consumed;
                            }

                            Source.BackPressure();
                        }
                        else if (!Zeroed() && !curJob.Zeroed() && !Source.Zeroed())
                        {
                            _logger.Error(
                                $"{GetType().Name}: {curJob.TraceDescription} consuming job: `{curJob.Description}' was unsuccessful, state = {curJob.State}");
                            curJob.State = IoJobMeta.JobState.Error;
                        }
                    }
                    catch (TaskCanceledException e)
                    {
                        _logger.Trace(e, Description);
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
                                _lastStat.ElapsedDelta() > TimeSpan.FromSeconds(10).TotalSeconds)
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
                                
                            curJob = Free(curJob);
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
            catch (NullReferenceException e) { _logger.Trace(e, Description);}
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (TimeoutException e) { _logger.Trace(e, Description); }
            catch (TaskCanceledException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            catch (Exception e)
            {
                _logger.Error(e, $"{GetType().Name}: Consumer {Description} dequeue returned with errors:");
            }

            return false;
        }

        /// <summary>
        /// Starts the processors
        /// </summary>
        public virtual async Task AssimilateAsync()
        {
            _logger.Trace($"{GetType().Name}: Assimulating {Description}");
            
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
                        await producers[i].OverBoostAsync().ConfigureAwait(false);

                        if (!producers[i].Result)
                            return;
                    }
                    
                }
            }, AsyncTasks.Token,TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach , TaskScheduler.Default);

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
                        await consumers[i].OverBoostAsync().ConfigureAwait(false);

                        if (!consumers[i].Result)
                            return;
                    }
                }
            }, AsyncTasks.Token, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach | TaskCreationOptions.PreferFairness, TaskScheduler.Default);

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
