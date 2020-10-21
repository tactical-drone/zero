using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models
{
    /// <summary>
    /// <see cref="CcProtocolBuffer"/> produces these <see cref="IIoJob"/>s
    ///
    /// These jobs contain a <see cref="Batch"/> of messages that were packed
    /// from <see cref="CcSubspaceMessage"/>s processed by <see cref="CcAdjunct"/>s from here: <see cref="CcAdjunct.ProcessAsync()"/>
    ///
    /// So, why not just put a <see cref="BlockingCollection{T}"/> on <see cref="CcAdjunct"/>
    /// and send the messages straight from <see cref="CcSubspaceMessage"/> to that Q?
    ///
    /// Ans: We need the telemetry provided by <see cref="IoZero{TJob}"/> to see what is going on. We also control
    /// resources, concurrency, many small events into larger ones etc. Also in the case of how UDP sockets work,
    /// this pattern fits perfectly with the strategy of doing the least amount of work (just buffering) on the edges:
    ///
    /// <see cref="CcAdjunct"/> -> <see cref="CcSubspaceMessage"/>     -> <see cref="CcProtocolBuffer"/> -> <see cref="IoConduit{TJob}"/>
    /// <see cref="BlockingCollection{T}"/> -                 instead of this we use                     <see cref="IoConduit{TJob}"/>
    /// <see cref="CcAdjunct"/> <- <see cref="CcProtocolMessage"/> <- <see cref="CcProtocolBuffer"/> <- <see cref="IoConduit{TJob}"/>
    /// </summary>
    public class CcProtocolMessage : IoSink<CcProtocolMessage>
    {
        
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="originatingSource">This message is forwarded by <see cref="CcProtocolBuffer"/></param>
        /// <param name="waitForConsumerTimeout"></param>
        public CcProtocolMessage(IoSource<CcProtocolMessage> originatingSource, int waitForConsumerTimeout = -1)
            : base("conduit", $"{nameof(CcProtocolMessage)}", originatingSource)
        {
            _waitForConsumerTimeout = waitForConsumerTimeout;
            _logger = LogManager.GetCurrentClassLogger();
        }


        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;
        private readonly int _waitForConsumerTimeout;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public volatile ValueTuple<IIoZero, IMessage,object, Proto.Packet>[] Batch;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            Batch = null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        /// <returns></returns>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {
            if (!await Source.ProduceAsync(async (producer, backPressure, ioZero, ioJob )=>
            {
                var _this = (CcProtocolMessage)ioJob;
                
                if (!await backPressure(ioJob, ioZero).ConfigureAwait(false))
                    return false;

                try
                {
                    _this.Batch = await ((CcProtocolBuffer) _this.Source).DequeueAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _this._logger.Fatal(e,$"MessageQueue.DequeueAsync failed: {_this.Description}"); 
                }

                _this.State = _this.Batch != null ? IoJobMeta.JobState.Produced : IoJobMeta.JobState.ProdCancel;

                return true;
            }, barrier, zeroClosure, this).ConfigureAwait(false))
            {
                State = IoJobMeta.JobState.ProduceTo;
            }
            
            //If the originatingSource gave us nothing, mark this production to be skipped            
            return State;
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>
        /// The state of the consumption
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            State = IoJobMeta.JobState.ConInlined;
            return ValueTask.FromResult(State);
        }
    }
}
