using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using Tangle.Net.Entity;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled.common.model.interop;

namespace zero.core.models.consumables.sources
{
    /// <summary>
    /// A producer that serves <see cref="Transaction"/>
    /// </summary>
    /// <seealso cref="zero.core.patterns.bushes.IoProducer{IoTangleTransaction}" />
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoProducer" />
    public class IoTangleMessageSource<TBlob> : IoProducer<IoTangleTransaction<TBlob>>, IIoProducer 
    {
        public IoTangleMessageSource(string id):base(10)//TODO config
        {
            //Saves forwarding producer, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();            
            Id = id;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;        
        
        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        public ConcurrentQueue<List<IIoTransactionModel<TBlob>>>  TxQueue = new ConcurrentQueue<List<IIoTransactionModel<TBlob>>>();

        public string Id;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => Upstream.Key;

        /// <summary>
        /// Description of this producer
        /// </summary>
        public override string Description => $"Broadcast tangle transaction from `{Upstream.Description}' to `{Id}'";

        public override string SourceUri => Upstream.SourceUri;

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => Upstream.IsOperational;        

        /// <summary>
        /// returns the forward producer
        /// </summary>
        /// <typeparam name="TFJob"></typeparam>
        /// <param name="id"></param>
        /// <param name="producer"></param>
        /// <param name="mallocMessage"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override IoForward<TFJob> GetRelaySource<TFJob>(string id, IoProducer<TFJob> producer = null,
            Func<object, IoConsumable<TFJob>> mallocMessage = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Closes this source
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public override void Close()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <returns>The async task</returns>        
        public override async Task<bool> ProduceAsync(Func<IIoProducer, Task<bool>> callback)
        {
            //Is the TCP connection up?
            if (!IsOperational)
            {
                return false;
            }

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
    }
}
