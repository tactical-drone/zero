using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.tangle.models.sources;

namespace zero.tangle.models
{
    /// <summary>
    /// Stores meta data used when consuming jobs of this kind
    /// </summary>    
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoProducer" />
    public class IoTangleTransaction<TKey> : IoConsumable<IoTangleTransaction<TKey>> 
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTangleTransaction{TKey}"/> class.
        /// </summary>
        /// <param name="source">The producer of these jobs</param>
        /// <param name="waitForConsumerTimeout">The time we wait for the producer before reporting it</param>
        public IoTangleTransaction(IoProducer<IoTangleTransaction<TKey>> source, int waitForConsumerTimeout = 0) : base("forward", $"{nameof(IoTangleTransaction<TKey>)}", source)
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
        public override async Task<State> ProduceAsync()
        {            
            await Producer.ProduceAsync(async producer =>
            {
                if (Producer.ProducerBarrier == null)
                {
                    ProcessState = State.ProdCancel;
                    return false;                    
                }

                if (!await Producer.ProducerBarrier.WaitAsync(_waitForConsumerTimeout, Producer.Spinners.Token))
                {
                    ProcessState = !Producer.Spinners.IsCancellationRequested ? State.ProduceTo : State.ProdCancel;
                    return false;
                }

                if (Producer.Spinners.IsCancellationRequested)
                {
                    ProcessState = State.ProdCancel;
                    return false;
                }
                
                Transactions = ((IoTangleTransactionProducer<TKey>)Producer).TxQueue.Take();

                ProcessState = State.Produced;

                return true;
            });

            //If the producer gave us nothing, mark this production to be skipped            
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
