using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.bushings
{
    /// <summary>
    /// Meta data about produced work that needs to be done
    /// </summary>
    /// <typeparam name="TJob">The job type</typeparam>
    public abstract class IoJob<TJob> : IoNanoprobe, IIoJob
        where TJob : IIoJob
    {
        /// <summary>
        /// static constructor
        /// </summary>
        static IoJob()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        protected IoJob(string desc, IoSource<TJob> source, int concurrencyLevel = 1) : base($"{nameof(IoJob<TJob>)}: {desc}", concurrencyLevel)
        {
            Source = source;
            _jobDesc = desc;
#if DEBUG
            var stateCount = Enum.GetNames(typeof(IoJobMeta.JobState)).Length;
            StateTransitionHistory = new IoQueue<IoStateTransition<IoJobMeta.JobState>>($"{nameof(StateTransitionHistory)}: {desc}", stateCount, concurrencyLevel, autoScale: true);
            _stateHeap = new($"{nameof(_stateHeap)}: {desc}", (Enum.GetNames(typeof(IoJobMeta.JobState)).Length * 2))
            {
                Malloc = static (_, _) => new IoStateTransition<IoJobMeta.JobState>() { FinalState = IoJobMeta.JobState.Halted }
            };
#endif
        }

        /// <summary>
        /// logger
        /// </summary>
        protected static readonly Logger _logger;

        /// <summary>
        /// A unique id for this work
        /// </summary>
        public long Id { get; protected set; }

        /// <summary>
        /// Work spanning multiple jobs
        /// </summary>
        public IIoJob PreviousJob { get; protected internal set; }

        private string _description;
        /// <summary>
        /// A description of this kind of work
        /// </summary>
        public override string Description
        {
            get
            {
                if(_description == null) 
                    return _description = $"{Source?.Description} | {_jobDesc}";
                return _description;
            }
        }

        /// <summary>
        /// A description of the job and work
        /// </summary>
        public virtual string TraceDescription => $"{Description}|#{Id} -";

        /// <summary>
        /// The ultimate source of workload
        /// </summary>
        public IIoSource Source { get; protected set; }

        /// <summary>
        /// The state transition history, sourced from <see  cref="IoZero{TJob}"/>
        /// </summary>
#if DEBUG
        public IoQueue<IoStateTransition<IoJobMeta.JobState>> StateTransitionHistory;
#else
        //public IoStateTransition<IoJobMeta.JobState>[] StateTransitionHistory;
#endif


        /// <summary>
        /// The current state
        /// </summary>
#if DEBUG
        private volatile IoStateTransition<IoJobMeta.JobState> _stateMeta;
#else
        private readonly IoStateTransition<IoJobMeta.JobState> _stateMeta = new();
#endif
        /// <summary>
        /// Enables async jobs to synchronize at certain parts of the pipeline, effectively chaining them into a unique processing order, ordered by <see cref="Id"/>
        /// </summary>
        protected internal volatile IoManualResetValueTaskSource<bool> ZeroRecovery = new(true);

        /// <summary>
        /// Uses <see cref="Source"/> to produce a job
        /// </summary>
        /// <param name="barrier">Congestion control</param>
        /// <param name="ioZero">The engine producing this job at the moment</param>
        /// <returns>The current state of the job</returns>
        public abstract ValueTask<IoJobMeta.JobState> ProduceAsync<T>(IIoSource.IoZeroCongestion<T> barrier, T ioZero);

        /// <summary>
        /// Initializes this instance for reuse from the heap
        /// </summary>
        /// <returns>This instance</returns>
#if DEBUG
        public virtual async ValueTask<IIoHeapItem> ReuseAsync()
#else
        public virtual ValueTask<IIoHeapItem> ReuseAsync()
#endif
        {
#if DEBUG
            await StateTransitionHistory.ZeroManagedAsync(static (s, @this) =>
            {
                @this._stateHeap.Return(s);
                return default;
            }, this).FastPath().ConfigureAwait(Zc);

            await StateTransitionHistory.ClearAsync().FastPath().ConfigureAwait(Zc);
#else
            _stateMeta.Set((int)IoJobMeta.JobState.Undefined);
#endif
            FinalState = State = IoJobMeta.JobState.Undefined;
            Id = -1;
            ZeroRecovery.Reset();
#if DEBUG
            return this;
#else
            return new ValueTask<IIoHeapItem>(this);
#endif
        }


        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
#if DEBUG
            _stateHeap.ZeroUnmanaged();
#endif

#if SAFE_RELEASE
            Source = null;
            PreviousJob = null;
#if DEBUG
            _stateHeap = null;
            StateTransitionHistory = null;
#endif
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

#if DEBUG
            if (_stateMeta != null)
                _stateHeap.Return(_stateMeta);

            await StateTransitionHistory.ZeroManagedAsync(static (s, @this) =>
            {
                @this._stateHeap.Return(s);
                return default;
            }, this, zero:true).FastPath().ConfigureAwait(Zc);


            await _stateHeap.ZeroManagedAsync((ioHeapItem, _) =>
            {
                ioHeapItem.ZeroManaged();
                return default;
            }, this).FastPath().ConfigureAwait(Zc);
#endif
            if (PreviousJob != null)
                await PreviousJob.Zero(this, $"{nameof(IoJob<TJob>)}: teardown").FastPath().ConfigureAwait(Zc);
        }

        /// <summary>
        /// Print the current state
        /// </summary>
        public void Print_stateMeta()
        {
            PrintState(_stateMeta);
        }

        /// <summary>
        /// Print the state transition history for this work
        /// </summary>
        public void PrintStateHistory()
        {
            var curState = _stateMeta.GetStartState();

            while (curState != null)
            {
                PrintState(curState);
                curState = curState.Next;
            }
        }

        /// <summary>
        /// Log formatting param that pads job ID strings
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_id_pad_size = 12;


        /// <summary>
        /// How long to wait for the consumer before timing out
        /// </summary>
        public virtual int WaitForConsumerTimeout { get; } = 2500;

        /// <summary>
        /// Log the state
        /// </summary>
        /// <param name="stateMeta">The instance to be printed</param>
        public void PrintState(IoStateTransition<IoJobMeta.JobState> stateMeta)
        {
            //if (isRepeat)
            //{
            //    _logger.Fatal("Production:{0} `{1}',[{2} {3}], [{4} ||{5}||], [{6} ({7})]",
            //        DateTimeOffset.FromUnixTimeMilliseconds(stateMeta.EnterTime),
            //        Description,
                    
            //        stateMeta.Prev == null ? stateMeta.DefaultPadded : stateMeta.Prev.PaddedStr(),
            //        (stateMeta.Lambda.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
            //        stateMeta.PaddedStr(),(stateMeta.Mu.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
            //        stateMeta.Next == null ? stateMeta.DefaultPadded : stateMeta.Next.PaddedStr(),
            //        (stateMeta.Delta.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
            //}
            //else
            //{
                _logger.Error("Production:{0} `{1}',{2} ({3}) ~> {4} ({5}) ~> {6} ({7})",
                    DateTimeOffset.FromUnixTimeMilliseconds(stateMeta.EnterTime),
                    Description,
                    
                    stateMeta.Prev == null ? stateMeta.DefaultPadded : stateMeta.Prev.PaddedStr(),
                    (stateMeta.Lambda.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    stateMeta.PaddedStr(),
                    (stateMeta.Mu.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),
                    
                    stateMeta.Next == null ? stateMeta.DefaultPadded : stateMeta.Next.PaddedStr(),
                    (stateMeta.Delta.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
            //}
        }

        /// <summary>
        /// The total amount of states
        /// </summary>
        public static readonly int StateMapSize = Enum.GetNames(typeof(IoJobMeta.JobState)).Length;

        /// <summary>
        /// A description of this job
        /// </summary>
        private readonly string _jobDesc;


        /// <summary>
        /// state heap
        /// </summary>
#if DEBUG
        //TODO
        private IoHeap<IoStateTransition<IoJobMeta.JobState>> _stateHeap;
#endif
        /// <summary>
        /// Final state
        /// </summary>
        public IoJobMeta.JobState FinalState { get; set; }

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public IoJobMeta.JobState State
        {
            get
            {
                return _stateMeta.Value;
            }
            set
            {
#if DEBUG
                //Update the previous state's exit time
                if (_stateMeta != null)
                {
                    _stateMeta.ExitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                    if (_stateMeta.Value == IoJobMeta.JobState.Halted && value != IoJobMeta.JobState.Undefined)
                    {
                        PrintStateHistory();
                        _stateMeta.Set((int)IoJobMeta.JobState.Race);
                        throw new ApplicationException($"{TraceDescription} Cannot transition from `{IoJobMeta.JobState.Halted}' to `{value}'");
                    }

                    Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);

                    if (_stateMeta.Value == value)
                        return;
                    
                    Interlocked.Add(ref Source.ServiceTimes[(int)_stateMeta.Value], _stateMeta.Mu);
                }
                else
                {
                    if (value != IoJobMeta.JobState.Undefined && !Zeroed())
                    {
                        PrintStateHistory();
                        throw new Exception($"{TraceDescription} First state transition history's first transition should be `{IoJobMeta.JobState.Undefined}', but is `{value}'");                        
                    }
                }
#endif

#if DEBUG
                //Allocate memory for a new current state
                var newState = _stateHeap.Take();
                if (newState == null)
                {
                    if (!Zeroed())
                        throw new OutOfMemoryException($"{Description}");

                    return;
                }

                newState.ConstructorAsync(_stateMeta, (int)value).FastPath().ConfigureAwait(Zc);

                _stateMeta = newState;
                StateTransitionHistory.EnqueueAsync(_stateMeta).FastPath().GetAwaiter().GetResult();
                
#else
                _stateMeta.ExitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _stateMeta.Set((int)value);
                _stateMeta.EnterTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
#endif
#if DEBUG
                //terminate
                if (value is IoJobMeta.JobState.Accept or IoJobMeta.JobState.Reject)
                {
                    FinalState = value;
                    State = IoJobMeta.JobState.Halted;
                }                
#endif
            }
        }
    }
}