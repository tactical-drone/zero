using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;

namespace zero.tangle.models.sources
{
    /// <summary>
    /// A upstream that serves <see cref="IoTangleTransaction{TKey}"/>
    /// </summary>
    /// <seealso cref="IoSource{TJob}" />
    /// <seealso cref="IIoSource" />
    public sealed class IoTangleTransactionSource<TKey> : IoSource<IoTangleTransaction<TKey>>, IIoSource 
    {
        public IoTangleTransactionSource(string destDescription, int prefetchSize):base(destDescription, false, prefetchSize, 2)//TODO config
        {
            //Saves forwarding upstream, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();
            _destDescription = destDescription;
            TxQueue = new BlockingCollection<List<IIoTransactionModel<TKey>>>(prefetchSize);
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
        public override string Key => UpstreamSource.Key;

        /// <summary>
        /// Description of this upstream
        /// </summary>
        public override string Description => $"{_destDescription}";


        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational() => false;

        /// <inheritdoc />        
        //public override IoConduit<TFJob> EnsureChannel<TFJob>(string id, IoSource<TFJob> channelSource = null,
        //    Func<object, IoLoad<TFJob>> jobMalloc = null)
        //{
        //    throw new NotImplementedException();
        //}

        //public override IoConduit<TFJob> GetChannel<TFJob>(string id)
        //{
        //    throw new NotImplementedException();
        //}
        public override void ZeroUnmanaged()
        {
            TxQueue.Dispose();

            base.ZeroUnmanaged();
            TxQueue = null;
        }
    }
}
