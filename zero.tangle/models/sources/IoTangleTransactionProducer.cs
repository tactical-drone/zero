using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.tangle.models.sources
{
    /// <summary>
    /// A producer that serves <see cref="IoTangleTransaction{TKey}"/>
    /// </summary>
    /// <seealso cref="zero.core.patterns.bushes.IoProducer{IoTangleTransaction}" />
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoProducer" />
    public sealed class IoTangleTransactionProducer<TKey> : IoProducer<IoTangleTransaction<TKey>>, IIoProducer 
    {
        public IoTangleTransactionProducer(string destDescription, int bufferSize):base(bufferSize)//TODO config
        {
            //Saves forwarding producer, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();
            _destDescription = destDescription;
            TxQueue = new BlockingCollection<List<IIoTransactionModel<TKey>>>(bufferSize);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;        
        
        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        public BlockingCollection<List<IIoTransactionModel<TKey>>> TxQueue;

        /// <summary>
        /// Describe the destination producer
        /// </summary>
        private readonly string _destDescription;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => Upstream.Key;

        /// <summary>
        /// Description of this producer
        /// </summary>
        public override string Description => $"{_destDescription}";

        /// <summary>
        /// The original source URI
        /// </summary>
        public override string SourceUri => Upstream.SourceUri;

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => Upstream.IsOperational;        

        /// <inheritdoc />        
        public override IoForward<TFJob> GetDownstreamArbiter<TFJob>(string id, IoProducer<TFJob> producer = null,
            Func<object, IoConsumable<TFJob>> jobMalloc = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Closes this source
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public override void Close()
        {
            TxQueue.Dispose();
        }

        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <returns>The async task</returns>        
        public override async Task<bool> ProduceAsync(Func<IIoProducer, Task<bool>> callback)
        {                        
            try
            {
                return await callback(this);
            }
            catch (TimeoutException)
            {
                return false;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Producer `{Description}' callback failed:");
                return false;
            }
        }

        /// <inheritdoc />        
        public override void ConfigureUpstream(IIoProducer producer)
        {
            Upstream = producer;            
        }

        public override void SetArbiter(IoProducerConsumer<IoTangleTransaction<TKey>> arbiter)
        {
            Arbiter = arbiter;
        }
    }
}
