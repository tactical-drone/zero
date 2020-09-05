using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Org.BouncyCastle.Bcpg;
using zero.cocoon.models.sources;
using zero.core.patterns.bushes;

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
        public Tuple<IMessage,object, Proto.Packet>[] Messages;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<JobState> ProduceAsync()
        {
            await Source.ProduceAsync(async producer =>
            {
                if (Source.ProducerBarrier == null)
                {
                    State = JobState.ProdCancel;
                    return false;
                }

                if (!await Source.ProducerBarrier.WaitAsync(_waitForConsumerTimeout, AsyncTasks.Token))
                {
                    State = Zeroed() ? JobState.ProdCancel : JobState.ProduceTo;
                    return false;
                }

                if (Zeroed())
                {
                    State = JobState.ProdCancel;
                    return false;
                }

                //if (((IoCcProtocolBuffer) Source).MessageQueue.Count > 0)
                {
                    Messages = ((IoCcProtocolBuffer)Source).MessageQueue.Take(AsyncTasks.Token);
                    State = JobState.Produced;
                }
                //else
                //{
                //    Messages = null;
                //    State = JobState.ProduceTo;
                //}
                
                return true;
            });

            //If the originatingSource gave us nothing, mark this production to be skipped            
            return State;
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>
        /// The state of the consumption
        /// </returns>
        public override Task<JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            State = JobState.ConInlined;
            return Task.FromResult(State);
        }
    }
}
