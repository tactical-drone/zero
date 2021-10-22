using System;
using System.Threading.Tasks;
using NLog;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using Logger = NLog.Logger;

namespace zero.core.core
{
    /// <inheritdoc />
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class IoNeighbor<TJob> : IoZero<TJob>
    where TJob : IIoJob
    {
        /// <summary>
        /// ConstructAsync
        /// </summary>
        /// <param name="node">The node this neighbor is connected to</param>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocJob">The callback that allocates new message buffer space</param>
        /// <param name="enableSync"></param>
        /// <param name="cascade"></param>
        public IoNeighbor(IoNode<TJob> node, IoNetClient<TJob> ioNetClient, Func<object, IIoNanite, IoSink<TJob>> mallocJob,
            bool enableSync, bool cascade = true)
            : base($"neighbor({ioNetClient?.Description})", ioNetClient, mallocJob, enableSync, cascade)
        {
            _logger = LogManager.GetCurrentClassLogger();
            Node = node;
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// The node this neighbor belongs to
        /// </summary>
        protected IoNode<TJob> Node;

        /// <summary>
        /// The Id of this neighbor
        /// </summary>
        /// <returns></returns>
        public virtual string Key => Source.Key;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            Node = null;
#endif
        }

        /// <summary>
        /// 
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }
    }
}
