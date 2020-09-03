﻿using System;
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
        public override async Task<State> ProduceAsync()
        {
            await Source.ProduceAsync(async producer =>
            {
                if (Source.ProducerBarrier == null)
                {
                    ProcessState = State.ProdCancel;
                    return false;
                }

                if (!await Source.ProducerBarrier.WaitAsync(_waitForConsumerTimeout, AsyncTasks.Token))
                {
                    ProcessState = Zeroed() ? State.ProdCancel : State.ProduceTo;
                    return false;
                }

                if (Zeroed())
                {
                    ProcessState = State.ProdCancel;
                    return false;
                }

                //if (((IoCcProtocolBuffer) Source).MessageQueue.Count > 0)
                {
                    Messages = ((IoCcProtocolBuffer)Source).MessageQueue.Take(AsyncTasks.Token);
                    ProcessState = State.Produced;
                }
                //else
                //{
                //    Messages = null;
                //    ProcessState = State.ProduceTo;
                //}
                
                return true;
            });

            //If the originatingSource gave us nothing, mark this production to be skipped            
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
