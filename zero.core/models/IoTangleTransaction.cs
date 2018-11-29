using System;
using System.Threading.Tasks;
using Tangle.Net.Entity;
using zero.core.models.producers;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.models
{
    /// <summary>
    /// Stores meta data used when consuming jobs of this kind
    /// </summary>
    /// <seealso cref="zero.core.patterns.bushes.IoConsumable{zero.core.models.IoTangleTransaction}" />
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
        }

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
        public override Task<State> ProduceAsync(IoProducable<IoTangleTransaction> fragment)
        {
            //Basically we just fetch the transaction through the producer
            Transaction = ((IoTangleMessageProducer)ProducerHandle).Load;

            //If the producer gave us nothing, mark this production to be skipped
            ProcessState = Transaction != null ? State.Produced : State.ProduceSkipped;
            return Task.FromResult(ProcessState);
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
