﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.consumables.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;

namespace zero.core.models.consumables
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
            JobDescription = $"tangle transaction from `{source.Description}'";
        }

        private readonly Logger _logger;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public List<IIoInteropTransactionModel> Transactions;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <param name="fragment"></param>
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<State> ProduceAsync(IoProducable<IoTangleTransaction> fragment)
        {
            ProcessState = State.Producing;
            await ProducerHandle.ProduceAsync(async producer =>
            {
                if (ProducerHandle.ProducerBarrier == null)
                {
                    ProcessState = State.ProduceCancelled;
                    return false;                    
                }

                if (!await ProducerHandle.ProducerBarrier.WaitAsync(0, ProducerHandle.Spinners.Token))
                {
                    ProcessState = !ProducerHandle.Spinners.IsCancellationRequested ? State.ProduceTo : State.ProduceCancelled;
                    return false;
                }

                if (ProducerHandle.Spinners.IsCancellationRequested)
                {
                    ProcessState = State.ProduceCancelled;
                    return false;
                }
                
                ((IoTangleMessageSource)ProducerHandle).TxQueue.TryDequeue(out Transactions);
                
                ProcessState = Transactions == null ? State.ProduceSkipped : State.Produced;                

                return true;
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
            ProcessState = State.ConsumeInlined;
            return Task.FromResult(ProcessState);
        }
    }    
}
