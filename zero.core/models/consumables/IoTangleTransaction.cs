﻿using System;
using System.Threading.Tasks;
using NLog;
using Tangle.Net.Entity;
using zero.core.consumables.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.models
{
    /// <summary>
    /// Stores meta data used when consuming jobs of this kind
    /// </summary>
    /// <seealso cref="IoTangleTransaction" />
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoProducer" />
    public sealed class IoTangleTransaction : IoConsumable<IoTangleTransaction>, IIoProducer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTangleTransaction"/> class.
        /// </summary>
        /// <param name="source">The producer of these jobs</param>
        public IoTangleTransaction(IoProducer<IoTangleTransaction> source)
        {
            ProducerHandle = source;
            _logger = LogManager.GetCurrentClassLogger();
            WorkDescription = "forward";
            JobDescription = "tangle transaction";
        }

        private readonly Logger _logger;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public Transaction Transaction;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <param name="fragment"></param>
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<State> ProduceAsync(IoProducable<IoTangleTransaction> fragment)
        {
                        
            await ProducerHandle.Produce(async producer =>
            {
                if (!await ProducerHandle.ProducerBarrier.WaitAsync(0, ProducerHandle.Spinners.Token))
                {
                    ProcessState = !ProducerHandle.Spinners.IsCancellationRequested ? State.ProduceTo : State.ProduceCancelled;
                    return Task.CompletedTask;
                }

                if (ProducerHandle.Spinners.IsCancellationRequested)
                {
                    ProcessState = State.ProduceCancelled;
                    return Task.CompletedTask;
                }

                //Basically we just fetch the transaction through the producer
                Transaction = ((IoTangleMessageSource)ProducerHandle).Load;
                ProcessState = Transaction != null ? State.Produced : State.ProduceSkipped;

                return Task.FromResult(Task.CompletedTask);
            });

            //If the producer gave us nothing, mark this production to be skipped            
            return ProcessState;
        }

        /// <summary>
        /// Set unprocessed data as more fragments.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public override void MoveUnprocessedToFragment()
        {
            throw new NotImplementedException();
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
            ProcessState = State.Consumed;
            return Task.FromResult(ProcessState);
        }
    }    
}
