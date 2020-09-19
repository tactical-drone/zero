using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.tangle.models.sources;

namespace zero.tangle.models
{
    /// <summary>
    /// Stores meta data used when consuming jobs of this kind
    /// </summary>    
    /// <seealso cref="IIoSource" />
    public class IoTangleTransaction<TKey> : IoLoad<IoTangleTransaction<TKey>> 
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTangleTransaction{TKey}"/> class.
        /// </summary>
        /// <param name="originatingSource">The originatingSource of these jobs</param>
        /// <param name="waitForConsumerTimeout">The time we wait for the originatingSource before reporting it</param>
        public IoTangleTransaction(IoSource<IoTangleTransaction<TKey>> originatingSource, int waitForConsumerTimeout = 0) 
            : base("forward", $"{nameof(IoTangleTransaction<TKey>)}", originatingSource)
        {
            _waitForConsumerTimeout = waitForConsumerTimeout;            
            _logger = LogManager.GetCurrentClassLogger();            
        }

        private readonly Logger _logger;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public List<IIoTransactionModel<TKey>> Transactions;

        /// <summary>
        /// How long to wait the consumer before logging it
        /// </summary>
        private readonly int _waitForConsumerTimeout;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>        
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {            
            await Source.ProduceAsync(async (producer, consumeSync, ioZero, ioJob) =>
            {
                if (!await consumeSync(ioJob, ioZero))
                    return false;
                
                Transactions = ((IoTangleTransactionSource<TKey>)Source).TxQueue.Take();

                State = IoJobMeta.JobState.Produced;

                return true;
            }, barrier, zeroClosure, this);

            //If the originatingSource gave us nothing, mark this production to be skipped            
            return State;
        }

        /// <summary>
        /// Consumes the job
        /// </summary>
        /// <returns>
        /// The state of the consumption
        /// </returns>
        public override ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            //No work is needed, we just mark the job as consumed. 
            State = IoJobMeta.JobState.ConInlined;
            return ValueTask.FromResult(State);
        }
    }    
}
