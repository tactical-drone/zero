using System;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using NLog;
using zero.cocoon.models.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models
{
    public class IoCcProtocolMessage : IoSink<IoCcProtocolMessage>
    {
        public IoCcProtocolMessage(IoSource<IoCcProtocolMessage> originatingSource, int waitForConsumerTimeout = -1)
            : base("channel", $"{nameof(IoCcProtocolMessage)}", originatingSource)
        {
            _waitForConsumerTimeout = waitForConsumerTimeout;
            _logger = LogManager.GetCurrentClassLogger();
        }


        private readonly Logger _logger;
        private readonly int _waitForConsumerTimeout;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public volatile Tuple<IIoZero, IMessage,object, Proto.Packet>[] Batch;

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            Batch = null;
        }

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
                var _this = (IoCcProtocolMessage)ioJob;
                
                if (!await backPressure(ioJob, ioZero).ConfigureAwait(false))
                    return false;

                try
                {
                    _this.Batch = await ((IoCcProtocolBuffer) _this.Source).DequeueAsync().ConfigureAwait(false);
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
        public override ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            State = IoJobMeta.JobState.ConInlined;
            return ValueTask.FromResult(State);
        }
    }
}
