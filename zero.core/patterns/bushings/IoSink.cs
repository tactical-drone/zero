using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
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
        /// Constructor
        /// </summary>
        protected IoSink(string sinkDesc, string jobDesc, IoSource<TJob> source, int concurrencyLevel = 1) : base(jobDesc, source, concurrencyLevel)
        {
            _sinkDesc = sinkDesc;
        }

        /// <summary>
        /// A description of the load
        /// </summary>
        private readonly string _sinkDesc;

#if DEBUG
        private string _description;
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
                if (_description == null)
                    return _description = $"{_sinkDesc} ~> {base.Description}";
                return _description;
#else
                return string.Empty;
#endif
            }
        }

        /// <summary>
        /// ZeroAsync handle
        /// </summary>
        public IIoZero IoZero { get; protected internal set; }

        /// <summary>
        /// Q handler
        /// </summary>
        public IoQueue<IoSink<TJob>>.IoZNode PrevJobQHook { get; internal set; }

        /// <summary>
        /// Heap constructor
        /// </summary>
        /// <returns></returns>
        public override async ValueTask<IIoHeapItem> ReuseAsync()
        {
            await base.ReuseAsync();
            IoZero = null;
            PrevJobQHook = null;
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
        protected internal abstract void AddRecoveryBits();

        /// <summary>
        /// Updates buffer meta data
        /// </summary>
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
        public void GenerateJobId()
        {
            Id = Source.NextJobIdSeed();
        }
    }
}
