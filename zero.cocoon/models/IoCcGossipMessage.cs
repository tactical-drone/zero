using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NLog;
using zero.core.models;
using zero.core.patterns.bushes;

namespace zero.cocoon.models
{
    public class IoCcGossipMessage<TKey> : IoMessage<IoCcGossipMessage<TKey>>
    {
        public IoCcGossipMessage(string jobDescription, string workDescription, IoProducer<IoCcGossipMessage<TKey>> producer) : base(jobDescription, workDescription, producer)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }


        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        public override Task<State> ProduceAsync()
        {
            throw new NotImplementedException();
        }

        public override Task<State> ConsumeAsync()
        {
            throw new NotImplementedException();
        }
    }
}
