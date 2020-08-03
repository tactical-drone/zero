using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.cocoon.models.sources;
using zero.core.patterns.bushes;

namespace zero.cocoon.models
{
    class IoCcProtocolMessage<TKey> : IoConsumable<IoCcProtocolMessage<TKey>>
    {
        public IoCcProtocolMessage(IoProducer<IoCcProtocolMessage<TKey>> source, int waitForConsumerTimeout = 0)
            : base("forward", $"{nameof(IoCcProtocolMessage<TKey>)}", source)
        {
            _waitForConsumerTimeout = waitForConsumerTimeout;
            _logger = LogManager.GetCurrentClassLogger();
        }


        private readonly Logger _logger;
        private readonly int _waitForConsumerTimeout;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public List<Tuple<IMessage,object>> Messages;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<State> ProduceAsync()
        {
            await Producer.ProduceAsync(async producer =>
            {
                if (Producer.ProducerBarrier == null)
                {
                    ProcessState = State.ProdCancel;
                    return false;
                }

                if (!await Producer.ProducerBarrier.WaitAsync(_waitForConsumerTimeout, Producer.Spinners.Token))
                {
                    ProcessState = !Producer.Spinners.IsCancellationRequested ? State.ProduceTo : State.ProdCancel;
                    return false;
                }

                if (Producer.Spinners.IsCancellationRequested)
                {
                    ProcessState = State.ProdCancel;
                    return false;
                }

                Messages = ((IoCcProtocol<TKey>)Producer).TxQueue.Take();

                ProcessState = State.Produced;

                return true;
            });

            //If the producer gave us nothing, mark this production to be skipped            
            return ProcessState;
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>
        /// The state of the consumption
        /// </returns>
        public override Task<State> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            ProcessState = State.ConInlined;
            return Task.FromResult(ProcessState);
        }
    }
}
