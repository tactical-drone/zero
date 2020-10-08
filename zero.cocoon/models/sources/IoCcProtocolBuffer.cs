using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.semaphore;

namespace zero.cocoon.models.sources
{
    public class IoCcProtocolBuffer : IoSource<IoCcProtocolMessage>
    {
        public IoCcProtocolBuffer(IIoSource ioSource,ArrayPool<Tuple<IIoZero, IMessage, object, Packet>> arrayPool, int prefetchSize, int concurrencyLevel) 
            : base(prefetchSize, concurrencyLevel)//TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();

            Upstream = ioSource;
            ArrayPoolProxy = arrayPool;

            MessageQueue = new ConcurrentQueue<Tuple<IIoZero, IMessage, object, Packet>[]>();
            
            _queuePressure = ZeroOnCascade(new IoZeroSemaphoreSlim(AsyncToken, $"{GetType().Name}: {nameof(_queuePressure)}", concurrencyLevel, 0, false,  false, true)).target;
            _queueBackPressure = ZeroOnCascade(new IoZeroSemaphoreSlim(AsyncToken, $"{GetType().Name}: {nameof(_queueBackPressure)}", concurrencyLevel, concurrencyLevel, false, false, true)).target;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Shared heap
        /// </summary>
        public ArrayPool<Tuple<IIoZero, IMessage, object, Packet>> ArrayPoolProxy { get; protected set; }

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        protected ConcurrentQueue<Tuple<IIoZero, IMessage, object, Packet>[]> MessageQueue;

        /// <summary>
        /// Sync used to access the Q
        /// </summary>
        private IoZeroSemaphoreSlim _queuePressure;

        /// <summary>
        /// Sync used to access the Q
        /// </summary>
        private IoZeroSemaphoreSlim _queueBackPressure;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => $"{nameof(IoCcProtocolBuffer)}({Upstream.Key})";

        /// <summary>
        /// Description of upstream channel
        /// </summary>
        //public override string Description
        //{
        //    get
        //    {
        //        try
        //        {
        //            if(!Zeroed())
        //                return $"{MessageQueue.Select(m => m.Length > 0 ? m.FirstOrDefault() : null).FirstOrDefault()?.Item2}";
        //        }
        //        catch (Exception e)
        //        {
        //            _logger.Trace(e,"Failed to get description:");
        //        }

        //        return null;
        //    }
        //} 

        public override string Description => Key;

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
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _queuePressure = null;
            _queueBackPressure = null;
            MessageQueue = null;
            ArrayPoolProxy = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            MessageQueue.Clear();
            _queuePressure.Zero();
            _queueBackPressure.Zero();
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Enqueue a batch
        /// </summary>
        /// <param name="item">The messages</param>
        /// <returns>Async task</returns>
        public async Task<bool> EnqueueAsync(Tuple<IIoZero, IMessage, object, Packet>[] item)
        {
            var backed = false;
            try
            {
                var backPressure = await _queueBackPressure.WaitAsync().ZeroBoostAsync().ConfigureAwait(false);
                backed = true;
                
                if (!backPressure)
                    return false;

                MessageQueue.Enqueue(item);

                _queuePressure.Release();
            }
            catch(Exception e)
            {
                if(backed)
                    _queueBackPressure.Release();

                _logger.Fatal(e, $"{nameof(EnqueueAsync)}: [FAILED], {MessageQueue.Count}, {_queuePressure}");
                return false;
            }

            return true;
        }


        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public async Task<Tuple<IIoZero, IMessage, object, Packet>[]> DequeueAsync()
        {
            try
            {
                Tuple<IIoZero, IMessage, object, Packet>[] batch = null;
                while (!Zeroed() && !MessageQueue.TryDequeue(out batch))
                {
                    var checkQ = await _queuePressure.WaitAsync().ZeroBoostAsync(oomCheck:false).ConfigureAwait(false);

                    if(Zeroed())
                        break;

                    _queueBackPressure.Release();

                    if (!checkQ)
                        break;
                }
                return batch;
            }
            catch (Exception e)
            {
                _logger.Trace(e,$"{Description}");
            }

            return null;
        }

        /// <summary>
        /// Queue count
        /// </summary>
        /// <returns>returns number of items in the q</returns>
        public int Count()
        {
            return MessageQueue.Count;
        }

        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <param name="barrier"></param>
        /// <param name="zeroClosure"></param>
        /// <param name="jobClosure"></param>
        /// <returns>The async task</returns>        
        public override async ValueTask<bool> ProduceAsync(
            Func<IIoSourceBase, Func<IIoJob, IIoZero, ValueTask<bool>>, IIoZero, IIoJob, Task<bool>> callback,
            Func<IIoJob, IIoZero, ValueTask<bool>> barrier = null, IIoZero zeroClosure = null, IIoJob jobClosure = null)
        {
            try
            {
                return await callback(this, barrier, zeroClosure, jobClosure).ConfigureAwait(false);
            }
            catch (TimeoutException e)
            {
                _logger.Trace(e, Description);
                return false;
            }
            catch (TaskCanceledException e)
            {
                _logger.Trace(e, Description);
                return false;
            }
            catch (OperationCanceledException e)
            {
                _logger.Trace(e, Description);
                return false;
            }
            catch (ObjectDisposedException e)
            {
                _logger.Trace(e, Description);
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
