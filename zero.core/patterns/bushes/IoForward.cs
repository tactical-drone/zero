using System;
using zero.core.patterns.bushes.contracts;

namespace zero.core.patterns.bushes
{
    public class IoForward<TJob>:IoProducerConsumer<TJob>, IIoForward
        where TJob : IIoWorker
    {
        public IoForward(string description, IoProducer<TJob> source, Func<object, IoConsumable<TJob>> mallocMessage) : base(description, source, mallocMessage)
        {
        }        
    }
}
