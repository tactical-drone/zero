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
    /// A upstream that serves <see cref="IoTangleTransaction{TKey}"/>
    /// </summary>
    /// <seealso cref="IoSource{TJob}" />
    /// <seealso cref="IIoSource" />
    public sealed class IoTangleTransactionSource<TKey> : IoSource<IoTangleTransaction<TKey>>, IIoSource 
    {
        public IoTangleTransactionSource(string destDescription, int bufferSize):base(bufferSize)//TODO config
        {
            //Saves forwarding upstream, to leech some values from it            
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
        /// Describe the destination upstream
        /// </summary>
        private readonly string _destDescription;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => SourceUri;

        /// <summary>
        /// Description of this upstream
        /// </summary>
        public override string Description => $"{_destDescription}";

        /// <summary>
        /// The original source URI
        /// </summary>
        public override string SourceUri => "";

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => false;

        /// <inheritdoc />        
        //public override IoChannel<TFJob> AttachProducer<TFJob>(string id, IoSource<TFJob> channelSource = null,
        //    Func<object, IoLoad<TFJob>> jobMalloc = null)
        //{
        //    throw new NotImplementedException();
        //}

        //public override IoChannel<TFJob> GetChannel<TFJob>(string id)
        //{
        //    throw new NotImplementedException();
        //}
        protected override void ZeroUnmanaged()
        {
            TxQueue.Dispose();

            base.ZeroUnmanaged();
            TxQueue = null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override Task ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }


        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <param name="barrier"></param>
        /// <returns>The async task</returns>        
        public override async Task<bool> ProduceAsync(Func<IIoSourceBase, Func<IIoJob, ValueTask<bool>>, Task<bool>> callback, Func<IIoJob, ValueTask<bool>> barrier = null)
        {                        
            try
            {
                return await callback(this, barrier);
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
                _logger.Error(e,$"Source `{Description}' callback failed:");
                return false;
            }
        }
    }
}
