using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Tangle.Net.Entity;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.models.producers
{
    /// <summary>
    /// A producer that serves <see cref="Transaction"/>
    /// </summary>
    /// <seealso cref="zero.core.patterns.bushes.IoProducer{IoTangleTransaction}" />
    /// <seealso cref="zero.core.patterns.bushes.contracts.IIoProducer" />
    public class IoTangleMessageProducer : IoProducer<IoTangleTransaction>, IIoProducer
    {
        public IoTangleMessageProducer(IoProducer<IoTangleMessage> producerHandle):base(50)
        {
            //Saves forwarding producer, to leech some values from it
            _producerHandle = producerHandle;            
        }

        /// <summary>
        /// The producer that we are forwarding from
        /// </summary>
        private readonly IoProducer<IoTangleMessage> _producerHandle;

        /// <summary>
        /// Used to load the value to be produced
        /// </summary>
        public Transaction Load;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override int Key => _producerHandle.Key;

        /// <summary>
        /// Description of this producer
        /// </summary>
        public override string Description => $"Broadcast tangle transaction from `{_producerHandle.Description}'";

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => _producerHandle.IsOperational;

        /// <summary>
        /// returns the forward producer
        /// </summary>
        /// <typeparam name="TFJob"></typeparam>
        /// <param name="producer"></param>
        /// <param name="mallocMessage"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override IoForward<TFJob> GetForwardProducer<TFJob>(IoProducer<TFJob> producer = null, Func<object, IoConsumable<TFJob>> mallocMessage = null)
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
        public override async Task<Task> Produce(Func<IIoProducer, Task<Task>> callback)
        {
            //Is the TCP connection up?
            if (!IsOperational) //TODO fix up
                throw new IOException("Socket has disconnected");

            try
            {
                return await callback(this);
            }
            catch (Exception e)
            {
                //Did the TCP connection drop?
                if (!IsOperational)
                    throw new IOException("Socket has disconnected", e);
                throw; //TODO fix this
            }
        }
    }
}
