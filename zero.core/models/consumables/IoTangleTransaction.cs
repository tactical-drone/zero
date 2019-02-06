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
        /// <param name="waitForProducerTimeout">The time we wait for the producer before reporting it</param>
        public IoTangleTransaction(IoProducer<IoTangleTransaction<TBlob>> source, int waitForProducerTimeout = 0)
        {
            _waitForProducerTimeout = waitForProducerTimeout;
            ProducerHandle = source;            
            _logger = LogManager.GetCurrentClassLogger();
            WorkDescription = "forward";
            JobDescription = $"tangle transaction from, `{source.Description}'";
        }

        private readonly Logger _logger;

        /// <summary>
        /// The transaction that is ultimately consumed
        /// </summary>
        public List<IIoTransactionModel<TBlob>> Transactions;

        private int _waitForProducerTimeout;

        /// <summary>
        /// Callback the generates the next job
        /// </summary>
        /// <param name="fragment"></param>
        /// <returns>
        /// The state to indicated failure or success
        /// </returns>
        public override async Task<State> ProduceAsync()
        {
            ProcessState = State.Producing;
            await ProducerHandle.ProduceAsync(async producer =>
            {
                if (ProducerHandle.ProducerBarrier == null)
                {
                    ProcessState = State.ProdCancel;
                    return false;                    
                }

                if (!await ProducerHandle.ProducerBarrier.WaitAsync(_waitForProducerTimeout, ProducerHandle.Spinners.Token))
                {
                    ProcessState = !ProducerHandle.Spinners.IsCancellationRequested ? State.ProduceTo : State.ProdCancel;
                    return false;
                }

                if (ProducerHandle.Spinners.IsCancellationRequested)
                {
                    ProcessState = State.ProdCancel;
                    return false;
                }
                
                ((IoTangleMessageSource<TBlob>)ProducerHandle).TxQueue.TryDequeue(out Transactions);
                
                ProcessState = Transactions == null ? State.ProStarting : State.Produced;                

                return true;
            });

            //If the producer gave us nothing, mark this production to be skipped            
            return ProcessState;
        }

        /// <summary>
        /// Set unprocessed data as more fragments.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        //public override void MoveUnprocessedToFragment()
        //{
        //    throw new NotImplementedException();
        //}

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
