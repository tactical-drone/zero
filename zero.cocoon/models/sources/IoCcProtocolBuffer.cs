using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models.sources
{
    sealed class IoCcProtocolBuffer : IoSource<IoCcProtocolMessage>, IIoSource
    {
        public IoCcProtocolBuffer(int bufferSize) : base(bufferSize)//TODO config
        {
            //Saves forwarding upstream, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();
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
        /// Keys this instance.
        /// </summary>
        public override string Key => this != ChannelSource ? $"{ChannelSource.Key}":$"{SourceUri}";

        /// <summary>
        /// Description of upstream channel
        /// </summary>
        public override string Description => $"{MessageQueue.Select(m=>m.Count > 0? m.FirstOrDefault() : null).FirstOrDefault()?.Item2}";
        //public override string Description => Key;

        /// <summary>
        /// The original source URI
        /// </summary>
        public override string SourceUri => this!=ChannelSource? ChannelSource?.SourceUri : $"chan://{GetType().Name}";

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => this == ChannelSource || (ChannelSource?.IsOperational??false);


        private bool _closed = false;
        
        /// <summary>
        /// Closes this source
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public override void Close()
        {
            lock (this)
            {
                if(_closed) return;
                _closed = true;

                _logger.Debug($"Closing `{Description}'");

                try
                {
                    Spinners.Cancel();
                    MessageQueue.Dispose();
                }
                catch (Exception e)
                {
                    _logger.Trace(e, "Close returned with errors");
                }
            }
            
        }

        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <returns>The async task</returns>        
        public override async Task<bool> ProduceAsync(Func<IIoSourceBase, Task<bool>> callback)
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
                _logger.Error(e, $"Source `{Description}' callback failed:");
                return false;
            }
        }
    }
}
