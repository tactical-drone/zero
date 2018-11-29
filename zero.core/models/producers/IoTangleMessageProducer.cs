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
    public class IoTangleMessageProducer : IoProducer<IoTangleTransaction>, IIoProducer
    {
        public IoTangleMessageProducer(IoProducer<IoTangleMessage> producerHandle):base(50)
        {
            _producerHandle = producerHandle;            
        }

        private readonly IoProducer<IoTangleMessage> _producerHandle;

        public Transaction Load;

        public override int Key => _producerHandle.Key;
        public override string Description => _producerHandle.Description;
        public override bool IsOperational => _producerHandle.IsOperational;

        public override IoForward<TFJob> GetForwardProducer<TFJob>(IoProducer<TFJob> producer = null, Func<object, IoConsumable<TFJob>> mallocMessage = null)
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            throw new NotImplementedException();
        }

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
