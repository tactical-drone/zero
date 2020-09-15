using System;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.cocoon.models.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models
{
    public class IoCcProtocolMessage : IoLoad<IoCcProtocolMessage>
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
        public volatile Tuple<IMessage,object, Proto.Packet>[] Messages;

        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            Messages = null;
        }

        protected override Task ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, ValueTask<bool>> barrier)
        {
            if (!await Source.ProduceAsync(async (producer, consumeSync )=>
            {
                if (!await consumeSync(this))
                    return false;

                try
                {
                    Messages = await ((IoCcProtocolBuffer) Source).DequeueAsync();
                }
                catch (Exception e)
                {
                    _logger.Fatal(e,
                        $"MessageQueue.DequeueAsync failed: {Description}"); 
                }

                State = Messages != null ? IoJobMeta.JobState.Produced : IoJobMeta.JobState.ProduceErr;

                return true;
            }, (Func<IIoJob, ValueTask<bool>>) barrier).ConfigureAwait(false))
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
        public override Task<IoJobMeta.JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            State = IoJobMeta.JobState.ConInlined;
            return Task.FromResult(State);
        }
    }
}
