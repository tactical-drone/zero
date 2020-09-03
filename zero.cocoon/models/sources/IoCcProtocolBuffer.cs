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
    public class IoCcProtocolBuffer : IoSource<IoCcProtocolMessage>, IIoSource
    {
        public IoCcProtocolBuffer(int bufferSize) : base(bufferSize)//TODO config
        {
            //Saves forwarding upstream, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();
            MessageQueue = new BlockingCollection<Tuple<IMessage, object, Packet>[]>(bufferSize);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        public BlockingCollection<Tuple<IMessage, object, Packet>[]> MessageQueue;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => $"{SourceUri}";

        /// <summary>
        /// Description of upstream channel
        /// </summary>
        public override string Description
        {
            get
            {
                try
                {
                    if(!Zeroed())
                        return $"{MessageQueue.Select(m => m.Length > 0 ? m.FirstOrDefault() : null).FirstOrDefault()?.Item2}";
                }
                catch (Exception e)
                {
                    _logger.Trace(e,"Failed to get description:");
                }

                return null;
            }
        } 
        //public override string Description => Key;

        /// <summary>
        /// The original source URI
        /// </summary>
        public override string SourceUri => $"chan://{GetType().Name}";

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => true;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            MessageQueue.Dispose();
             base.ZeroUnmanaged();

#if SAFE_RELEASE
            MessageQueue = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            while(MessageQueue.Take() != null){}
            base.ZeroManaged();
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
            catch (ObjectDisposedException)
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
