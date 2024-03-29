﻿using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

#if DEBUG
            StateHeap = new($"{nameof(IoJob<TJob>)}.{nameof(StateHeap)}:", short.MaxValue, static (_, _) => new IoStateTransition<IoJobMeta.JobState> { FinalState = IoJobMeta.JobState.Halted })
            {
                PopAction = (nextState, context) =>
                {
                    var (prevState, newStateId) = (ValueTuple<IoStateTransition<IoJobMeta.JobState>, int>)context;

                    nextState.ExitTime = nextState.EnterTime = Environment.TickCount;

                    nextState.Next = null;
                    nextState.Prev = null;

                    if (prevState != null)
                    {
                        nextState.Prev = prevState;
                        prevState.Next = nextState;
                    }

                    nextState.Set(newStateId);
                }
            };
#endif
        }

        /// <summary>
        /// sentinel
        /// </summary>
        protected IoJob()
        {
            
        }
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoJob(string desc, IIoSource source, int concurrencyLevel = 1) : base($"{nameof(IoJob<TJob>)}: {desc}", concurrencyLevel)
        {
            Source = source;

#if DEBUG
            _jobDesc = desc;
            StateTransitionHistory = new IoQueue<IoStateTransition<IoJobMeta.JobState>>($"{nameof(StateTransitionHistory)}: {desc}", 64, concurrencyLevel, IoQueue<IoStateTransition<IoJobMeta.JobState>>.Mode.DynamicSize);
#else
            _jobDesc = string.Empty;
#endif

            ZeroRecovery = new IoManualResetValueTaskSource<bool>(true);
        }

        /// <summary>
        /// logger
        /// </summary>
        // ReSharper disable once InconsistentNaming
        // ReSharper disable once StaticMemberInGenericType
        protected static readonly Logger _logger;

        private long _id;
        /// <summary>
        /// A unique id for this work
        /// </summary>
        public long Id {
            get => Interlocked.Read(ref _id);
            protected set => Interlocked.Exchange(ref _id, value);
        }

        /// <summary>
        /// Work spanning multiple jobs
        /// </summary>
        public IIoJob PreviousJob { get; internal set; }

        /// <summary>
        /// A description of this kind of work
        /// </summary>
#if DEBUG
        public override string Description => $"{_jobDesc}, {StateTransitionHistory?.Tail?.Value}";
#else
        public override string Description => string.Empty;
#endif


        /// <summary>
        /// A description of the job and work
        /// </summary>
#if DEBUG
        public virtual string TraceDescription => $"{Description}|#{Id} -";
#else
        public virtual string TraceDescription => string.Empty;
#endif

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
        protected internal IoManualResetValueTaskSource<bool> ZeroRecovery;

        /// <summary>
        /// Indicates that for a particular run, the recovery be disabled. Jobs that know mark other jobs as recovery redundant    
        /// </summary>
        protected internal bool EnableRecoveryOneshot;

        /// <summary>
        /// Uses <see cref="Source"/> to produce a job
        /// </summary>
        /// <param name="ioZero">The engine producing this job at the moment</param>
        /// <returns>The current state of the job</returns>
        public abstract ValueTask<IoJobMeta.JobState> ProduceAsync<T>(T ioZero);

        /// <summary>
        /// Initializes this instance for reuse from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public virtual
#if DEBUG
            async 
#endif
        ValueTask<IIoHeapItem> HeapPopAsync(object context)
        {
            try
            {
                //_logger.Fatal($"POP - {nameof(HeapPopAsync)}: <#{Serial}>[{Id}] - {Description}");
                FinalState = IoJobMeta.JobState.Undefined;
#if DEBUG
                await StateTransitionHistory.ZeroManagedAsync(static (s, @this) =>
                {
                    StateHeap.Return(s.Value);
                    return new ValueTask(Task.CompletedTask);
                }, this).FastPath();

                await StateTransitionHistory.ClearAsync().FastPath();
                Interlocked.Exchange(ref _stateMeta, null);
#else
                _stateMeta.Set((int)IoJobMeta.JobState.Undefined);
#endif
                Debug.Assert(PreviousJob == null);
                PreviousJob = null;
                Id = -1;
                ZeroRecovery.Reset();
                Volatile.Write(ref EnableRecoveryOneshot, false);

#if DEBUG
                return this;
#else
                return new ValueTask<IIoHeapItem>(this);
#endif
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Error(e, $"{nameof(HeapPopAsync)}:");
            }

            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void HeapPushAction()
        {
            //_logger.Fatal($"PUSH - {nameof(HeapPush)}: <#{Serial}>[{Id}]; state = {State}  - {Description}");
        }

        /// <summary>
        /// Default empty heap constructor
        /// </summary>
        /// <param name="context"></param>
        /// <returns>A task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual ValueTask<IIoHeapItem> HeapConstructAsync(object context)
        {
            return new ValueTask<IIoHeapItem>(this);
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            Source = null;
            PreviousJob = null;
#if DEBUG
            StateTransitionHistory = null;
#endif
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

#if DEBUG
            await StateTransitionHistory.ZeroManagedAsync(static (s, @this) =>
            {
                StateHeap.Return(s.Value);
                return new ValueTask(Task.CompletedTask);
            }, this, zero:true).FastPath();
#endif
            if (PreviousJob != null)
                await PreviousJob.DisposeAsync(this, $"cascade;  {ZeroReason}").FastPath();
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
        public void PrintStateHistory(string tag)
        {
            var curState = _stateMeta?.GetStartState();

            _logger.Warn($"[{tag}] dumpstate ~> {Description}");
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

            //_logger.Error("Production:{0} `{1}',{2} ({3}) ~> {4} ({5})",
            //    DateTimeOffset.FromUnixTimeMilliseconds(stateMeta.EnterTime),
            //    Description,

            //    stateMeta.Prev == null ? stateMeta.DefaultPadded : stateMeta.Prev.PaddedStr(),
            //    (stateMeta.Lambda.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size),

            //    stateMeta.PaddedStr(),
            //    (stateMeta.Mu.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
#if DEBUG
            _logger.Warn($"[{stateMeta.Id}] {stateMeta.DefaultPadded}");
#else
            _logger.Warn($"{stateMeta.DefaultPadded}");
#endif

            //stateMeta.Next == null ? stateMeta.DefaultPadded : stateMeta.Next.PaddedStr(),
            //(stateMeta.Delta.ToString(CultureInfo.InvariantCulture) + " ms ").PadLeft(parm_id_pad_size));
            //}
        }

        /// <summary>
        /// The total amount of states
        /// </summary>
        // ReSharper disable once StaticMemberInGenericType
        public static readonly int StateMapSize = Enum.GetNames(typeof(IoJobMeta.JobState)).Length;

        /// <summary>
        /// A description of this job
        /// </summary>
        private readonly string _jobDesc;



#if DEBUG
        /// <summary>
        /// state heap
        /// </summary>
        private static readonly IoHeap<IoStateTransition<IoJobMeta.JobState>> StateHeap;
#endif
        /// <summary>
        /// Final state
        /// </summary>
        public IoJobMeta.JobState FinalState { get; set; }


        public async ValueTask<IoJobMeta.JobState> SetStateAsync(IoJobMeta.JobState value)
        {
            try
            {
#if DEBUG
                //Update the previous state's exit time
                if (_stateMeta != null && !Zeroed())
                {
                    var s = _stateMeta.Value;
                    if (value == IoJobMeta.JobState.Undefined && s == IoJobMeta.JobState.Consuming)
                    {
                        //_stateMeta.Set((int)IoJobMeta.JobState.Race);
                        PrintStateHistory("SetStateAsync - c");
                        throw new ApplicationException(
                            $"(CONSUME!) Cannot transition from `{IoJobMeta.JobState.Halted}' to `{value}', had = {s}");
                    }

                    if (_stateMeta.Value == IoJobMeta.JobState.Halted && value != IoJobMeta.JobState.Undefined)
                    {
                        //_stateMeta.Set((int)IoJobMeta.JobState.Race);
                        PrintStateHistory("SetStateAsync");
                        throw new ApplicationException(
                            $"Cannot transition from `{_stateMeta.Value}' to `{value}', had = {s}");
                    }

                    if (value < _stateMeta.Value)
                    {
                        PrintStateHistory("RACE");
                        throw new ApplicationException(
                            $"Cannot transition from `{_stateMeta.Value}' to `{value}', had = {s}");
                    }

                    if (_stateMeta.Value == value)
                        return value;

                    _stateMeta.ExitTime = Environment.TickCount;
#if DEBUG
                    _stateMeta.Id = Id;

                    if ((_stateMeta.Prev?.Id ?? Id) != Id)
                    {
                        PrintStateHistory("Id");
                    }
#endif

                    if (Source != null)
                    {
                        Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);
                        Interlocked.Add(ref Source.ServiceTimes[(int)_stateMeta.Value], _stateMeta.Mu);
                    }
                }
                //else
                //{
                //    if (value != IoJobMeta.JobState.Produced && !Zeroed())
                //    {
                //        PrintStateHistory();
                //        throw new Exception(
                //            $"{TraceDescription} First state transition history's first transition should be `{IoJobMeta.JobState.Undefined}', but is `{value}'");
                //    }
                //}

                //Allocate memory for a new current state
                var newState = StateHeap.Take((_stateMeta, (int)value));

                if (newState.Next != null)
                {
                    _logger.Fatal($"POPPED <#{Serial}>[{Id}]; state[{newState.Id}] = {newState}, next[{newState.Next.Id}] = {newState.Next}");
                    PrintStateHistory("POPPED");
                }

                Debug.Assert(newState.Next == null);

                if (newState == null)
                {
                    if (!Zeroed())
                        throw new OutOfMemoryException($"{Description}");

                    return value;
                }

                _stateMeta = newState;
                if(StateTransitionHistory != null)
                    await StateTransitionHistory.EnqueueAsync(_stateMeta).FastPath();
#else
                _stateMeta.ExitTime = Environment.TickCount;
                Interlocked.Increment(ref Source.Counters[(int)_stateMeta.Value]);
                Interlocked.Add(ref Source.ServiceTimes[(int)_stateMeta.Value], _stateMeta.Mu);
                _stateMeta.Set((int)value);
                _stateMeta.EnterTime = Environment.TickCount;
#endif
                //sentinel 
                if (value is IoJobMeta.JobState.Accept or IoJobMeta.JobState.Reject or IoJobMeta.JobState.Recovering)
                {
                    FinalState = value;
                    //_logger.Error($"<#{Serial}:[{Id}]; SENTINEL {FinalState} <- {Source.Description}");
                    await SetStateAsync(IoJobMeta.JobState.Halted).FastPath();
                }
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(SetStateAsync)}: Setting state failed for {Description}");
            }

            return value;
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>The state of the consumption</returns>
        public abstract ValueTask<IoJobMeta.JobState> ConsumeAsync();

        /// <summary>
        /// Gets and sets the state of the work
        /// </summary>
        public IoJobMeta.JobState State => _stateMeta?.Value??IoJobMeta.JobState.Undefined;
    }
}