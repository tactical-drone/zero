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
        public override async Task<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {
            if (!await Source.ProduceAsync(async (producer, consumeSync, ioZero, ioJob )=>
            {
                var _this = (IoCcProtocolMessage)ioJob;
                
                if (!await consumeSync(ioJob, ioZero))
                    return false;

                try
                {
                    _this.Messages = await ((IoCcProtocolBuffer) _this.Source).DequeueAsync();
                }
                catch (Exception e)
                {
                    _this._logger.Fatal(e,
                        $"MessageQueue.DequeueAsync failed: {_this.Description}"); 
                }

                _this.State = _this.Messages != null ? IoJobMeta.JobState.Produced : IoJobMeta.JobState.ProduceErr;

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
        public override Task<IoJobMeta.JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            State = IoJobMeta.JobState.ConInlined;
            return Task.FromResult(State);
        }
    }
}
