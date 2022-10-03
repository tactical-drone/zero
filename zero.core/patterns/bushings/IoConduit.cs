using System;
using System.Runtime.CompilerServices;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings
{
    /// <summary>
    /// Used as a simple job forwarding source consumer queue, by <see cref="IoZero{TJob}"/>
    /// </summary>
    /// <typeparam name="TJob">The type of the job.</typeparam>
    /// <seealso cref="IoZero{TJob}" />
    /// <seealso cref="IIoConduit" />
    public class IoConduit<TJob>:IoZero<TJob>, IIoConduit
        where TJob : IIoJob
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoConduit{TJob}"/> class.
        /// </summary>
        /// <param name="description">A description of the channel destination</param>
        /// <param name="upstream">Upstream source</param>
        /// <param name="source">The source of the work to be done</param>
        /// <param name="mallocJob">A callback to malloc individual consumer jobs from the heap</param>
        public IoConduit(string description, IIoSource upstream, IoSource<TJob> source, Func<object, IIoNanite, IoSink<TJob>> mallocJob) : base(description, source, mallocJob, false, false)
        {
            UpstreamSource = upstream;
        }

        /// <summary>
        /// Upstream source
        /// </summary>
        public IIoSource UpstreamSource { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Zeroed()
        {
            return base.Zeroed() || Source.Zeroed() || (UpstreamSource?.Zeroed()??false);
        }
    }
}
