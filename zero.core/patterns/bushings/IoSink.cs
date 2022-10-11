using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

namespace zero.core.patterns.bushings
{
    /// <summary>
    /// Sink where <see cref="IoJob{TJob}"/> is consumed.
    /// </summary>
    /// <typeparam name="TJob">The type of the job</typeparam>
    public abstract class IoSink<TJob> : IoJob<TJob>
        where TJob : IIoJob

    {
        /// <summary>
        /// sentinel
        /// </summary>
        public IoSink()
        {
            
        }
        /// <summary>
        /// Constructor
        /// </summary>
        protected IoSink(string sinkDesc, string jobDesc, IoSource<TJob> source, int concurrencyLevel = 1) : base(jobDesc, source, concurrencyLevel)
        {
#if DEBUG
            _sinkDesc = sinkDesc;   
#endif
        }

#if DEBUG
        /// <summary>
        /// A description of the load
        /// </summary>
        private readonly string _sinkDesc;
#endif

        /// <inheritdoc />
        /// <summary>
        /// The overall description of the work that needs to be done and the job that is doing it
        /// </summary>
        //public override string ProductionDescription => $"{Source.ChannelSource?.Description} {Source.Description} {LoadDescription} {base.Description}";
        public override string Description
        {
            get
            {
#if DEBUG
                return $"<#{Serial}>[{Id}] {_sinkDesc} ~> {base.Description}";
#else
                return string.Empty;
#endif
            }
        }

        /// <summary>
        /// ZeroAsync handle
        /// </summary>
        public IoZero<TJob> IoZero { get; protected internal set; }

        /// <summary>
        /// Q handler
        /// </summary>
        protected internal IoQueue<IoSink<TJob>>.IoZNode FragmentIdx;

        /// <summary>
        /// Heap constructor
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async ValueTask<IIoHeapItem> HeapPopAsync(object context)
        {
            await base.HeapPopAsync(context).FastPath();
            FragmentIdx = null;
            return this;
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>The state of the consumption</returns>
        public abstract ValueTask<IoJobMeta.JobState> ConsumeAsync();

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            IoZero = null;
#endif
        }

        /// <summary>
        /// ZeroAsync managed
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// Handle fragmented jobs
        /// </summary>
        protected internal abstract ValueTask AddRecoveryBitsAsync();

        /// <summary>
        /// Updates buffer meta data
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected internal abstract bool ZeroEnsureRecovery();

        /// <summary>
        /// Used to debug
        /// </summary>
        /// <returns></returns>
        public virtual bool Verify([CallerMemberName] string desc = "", [CallerLineNumber] int sourceLineNumber = 0)
        {
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GenerateJobId()
        {
            if (Id < 0)
                return Id = Source.NextJobIdSeed();
            return Id;
        }
    }
}
