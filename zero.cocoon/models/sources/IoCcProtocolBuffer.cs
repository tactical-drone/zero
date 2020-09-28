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
using zero.core.patterns.semaphore;

namespace zero.cocoon.models.sources
{
    public class IoCcProtocolBuffer : IoSource<IoCcProtocolMessage>
    {
        public IoCcProtocolBuffer(IIoSource ioSource,ArrayPool<Tuple<IMessage, object, Packet>> arrayPool, int prefetchSize, int concurrencyLevel) 
            : base(prefetchSize, concurrencyLevel)//TODO config
        {
            Upstream = ioSource;
            ArrayPoolProxy = arrayPool;
            //Saves forwarding upstream, to leech some values from it            
            _logger = LogManager.GetCurrentClassLogger();
            MessageQueue = new ConcurrentQueue<Tuple<IMessage, object, Packet>[]>();
            
            _queuePressure = new IoZeroSemaphoreSlim(AsyncTokenProxy, $"{GetType().Name}: {nameof(_queuePressure)}", 1000, 0, 4, true, 1, false, true);
            //_queuePressure = ZeroOnCascade(new IoAutoMutex()).target;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Shared heap
        /// </summary>
        public ArrayPool<Tuple<IMessage, object, Packet>> ArrayPoolProxy { get; protected set; }

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        protected ConcurrentQueue<Tuple<IMessage, object, Packet>[]> MessageQueue;

        /// <summary>
        /// Sync used to access the Q
        /// </summary>
        private IoZeroSemaphoreSlim _queuePressure;

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
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _queuePressure = default;
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
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Enqueue a batch
        /// </summary>
        /// <param name="item">The messages</param>
        /// <returns>Async task</returns>
        public Task<bool> EnqueueAsync(Tuple<IMessage, object, Packet>[] item)
        {
            try
            {
                //await _queueBackPressure.WaitAsync(AsyncTasks.Token).ConfigureAwait(false);
                MessageQueue.Enqueue(item);
                _queuePressure.Release();
            }
            catch(Exception e)
            {
                _logger.Trace(e, $"{nameof(EnqueueAsync)}: [FAILED], {MessageQueue.Count}, {_queuePressure}");
                return Task.FromResult(false);
            }

            return Task.FromResult(true);
        }


        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public async Task<Tuple<IMessage, object, Packet>[]> DequeueAsync()
        {
            try
            {
                Tuple<IMessage, object, Packet>[] batch = null;
                while (!Zeroed() && !MessageQueue.TryDequeue(out batch))
                {
                    await _queuePressure.WaitAsync().ConfigureAwait(false);
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
