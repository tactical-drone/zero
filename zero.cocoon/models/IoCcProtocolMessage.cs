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

        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();
            Messages = null;
        }

        protected override void ZeroManaged()
        {
            base.ZeroManaged();
        }

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<JobState> ProduceAsync(Func<IoJob<IoCcProtocolMessage>, ValueTask<bool>> barrier)
        {
            await Source.ProduceAsync(async producer =>
            {
                if (!await barrier(this))
                    return false;

                //if (((IoCcProtocolBuffer) Source).MessageQueue.Count > 0)
                {

                    try
                    {
                        Messages = ((IoCcProtocolBuffer)Source).MessageQueue.Take(AsyncTasks.Token);
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e, $"MessageQueue.Take failed: {Description}");//TODO why are we not getting this warning?
                    }

                    State = JobState.Produced;
                }
                //else
                //{
                //    Messages = null;
                //    State = JobState.ProduceTo;
                //}
                
                return true;
            }).ConfigureAwait(false);

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
