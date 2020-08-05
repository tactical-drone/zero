using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models.sources
{
    class IoCcProtocolBuffer : IoProducer<IoCcProtocolMessage>, IIoProducer
    {
        public IoCcProtocolBuffer(string chanDesc, int bufferSize) : base(bufferSize)//TODO config
        {
            //Saves forwarding upstream, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();
            _chanDesc = chanDesc;
            MessageQueue = new BlockingCollection<List<Tuple<IMessage, object, Packet>>>(bufferSize);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        public BlockingCollection<List<Tuple<IMessage, object, Packet>>> MessageQueue;

        /// <summary>
        /// Describe the channel
        /// </summary>
        private readonly string _chanDesc;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => this != ChannelProducer ? $"{ChannelProducer.Key}":$"{Description}";

        /// <summary>
        /// Description of upstream channel
        /// </summary>
        public override string Description => $"{_chanDesc}";

        /// <summary>
        /// The original source URI
        /// </summary>
        public override string SourceUri => this!=ChannelProducer? ChannelProducer?.SourceUri : $"chan://{GetType().Name}";

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => this == ChannelProducer || (ChannelProducer?.IsOperational??false);

        /// <inheritdoc />        
        public override IoChannel<TFJob> AttachProducer<TFJob>(string id, IoProducer<TFJob> channelProducer = null,
            Func<object, IoConsumable<TFJob>> jobMalloc = null)
        {
            throw new NotImplementedException();
        }

        public override IoChannel<TFJob> GetChannel<TFJob>(string id)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Closes this source
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public override void Close()
        {
            MessageQueue.Dispose();
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
                _logger.Error(e, $"Producer `{Description}' callback failed:");
                return false;
            }
        }
    }
}
