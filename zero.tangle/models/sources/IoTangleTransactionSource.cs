using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.tangle.models.sources
{
    /// <summary>
    /// A upstream that serves <see cref="IoTangleTransaction{TKey}"/>
    /// </summary>
    /// <seealso cref="IoSource{TJob}" />
    /// <seealso cref="IIoSource" />
    public sealed class IoTangleTransactionSource<TKey> : IoSource<IoTangleTransaction<TKey>>, IIoSource 
    {
        public IoTangleTransactionSource(string destDescription, int prefetchSize):base(destDescription, prefetchSize, 2)//TODO config
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
        public override bool IsOperational => false;

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

        /// <summary>
        /// zero managed
        /// </summary>
        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }


        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <param name="jobClosure"></param>
        /// <param name="barrier"></param>
        /// <param name="nanite"></param>
        /// <returns>The async task</returns>        
        public override async ValueTask<bool> ProduceAsync<T>(
            Func<IIoNanite, Func<IIoJob, T, ValueTask<bool>>, T, IIoJob, ValueTask<bool>> callback,
            IIoJob jobClosure = null,
            Func<IIoJob, T, ValueTask<bool>> barrier = null,
            T nanite = default)
        {                        
            try
            {
                return await callback(this, barrier, nanite, jobClosure);
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
