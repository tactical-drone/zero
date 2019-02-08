using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.models.consumables.sources;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled.common.model.interop;

namespace zero.core.models.consumables
{
    /// <summary>
    /// Stores meta data used when consuming jobs of this kind
    /// </summary>    
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoProducer" />
    public sealed class IoTangleTransaction<TBlob> : IoConsumable<IoTangleTransaction<TBlob>> 
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTangleTransaction{TBlob}"/> class.
        /// </summary>
        /// <param name="source">The producer of these jobs</param>
        /// <param name="waitForConsumerTimeout">The time we wait for the producer before reporting it</param>
        public IoTangleTransaction(IoProducer<IoTangleTransaction<TBlob>> source, int waitForConsumerTimeout = 0) : base("forward", $"tangle transaction from: `{source.Description}'", source)
        {
            _waitForConsumerTimeout = waitForConsumerTimeout;            
            _logger = LogManager.GetCurrentClassLogger();            
        }

        private readonly Logger _logger;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public List<IIoTransactionModel<TBlob>> Transactions;

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
            ProcessState = State.Producing;
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
                
                ((IoTangleTransactionProducer<TBlob>)Producer).TxQueue.TryDequeue(out Transactions);
                
                ProcessState = Transactions == null ? State.ProStarting : State.Produced;                

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
