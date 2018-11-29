using System;
using System.Threading.Tasks;
using Tangle.Net.Entity;
using zero.core.models.producers;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.models
{
    public sealed class IoTangleTransaction : IoConsumable<IoTangleTransaction>, IIoProducer
    {
        public IoTangleTransaction(IoProducer<IoTangleTransaction> source)
        {
            ProducerHandle = source;
        }

        public Transaction Transaction;

        public override Task<State> ProduceAsync(IoProducable<IoTangleTransaction> fragment)
        {
            Transaction = ((IoTangleMessageProducer)ProducerHandle).Load;
            ProcessState = State.Produced;
            return Task.FromResult(ProcessState);
        }

        public override void MoveUnprocessedToFragment()
        {
            throw new NotImplementedException();
        }

        public override Task<State> ConsumeAsync()
        {
            ProcessState = State.Consumed;
            return Task.FromResult(ProcessState);
        }
    }    
}
