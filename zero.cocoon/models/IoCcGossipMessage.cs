using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NLog;
using zero.core.models;
using zero.core.patterns.bushes;

namespace zero.cocoon.models
{
    public class IoCcGossipMessage : IoMessage<IoCcGossipMessage>
    {
        public IoCcGossipMessage(string jobDescription, string workDescription, IoProducer<IoCcGossipMessage> producer) : base(jobDescription, workDescription, producer)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }


        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        public override async Task<State> ProduceAsync()
        {
            return ProcessState = State.ProduceTo;
        }

        public override async Task<State> ConsumeAsync()
        {
            return ProcessState = State.Consumed;
        }
    }
}
