﻿using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using Proto;
using zero.cocoon.autopeer;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.semaphore;

namespace zero.cocoon.models.sources
{
    /// <summary>
    /// Used as a source of unmarshalled protobuf msgs by <see cref="IoConduit{TJob}"/> for <see cref="CcAdjunct"/>
    /// </summary>
    public class CcProtocolBuffer : IoSource<CcProtocolMessage>
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="ioSource">The source of this model</param>
        /// <param name="arrayPool">Used to establish a pool</param>
        /// <param name="prefetchSize">Initial job prefetch from source</param>
        /// <param name="concurrencyLevel">The level of concurrency when producing and consuming on this source</param>
        public CcProtocolBuffer(IIoSource ioSource,ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>> arrayPool, int prefetchSize, int concurrencyLevel) 
            : base(prefetchSize, concurrencyLevel)//TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();

            Upstream = ioSource;
            ArrayPoolProxy = arrayPool;

            MessageQueue = new ConcurrentQueue<ValueTuple<IIoZero, IMessage, object, Packet>[]>();
    
            var enableFairQ = false;
            var enableDeadlockDetection = true;
#if RELEASE
            enableDeadlockDetection = false;
#endif
            
            _queuePressure = ZeroOnCascade(new IoZeroSemaphoreSlim(AsyncTasks, $"{GetType().Name}: {nameof(_queuePressure)}", concurrencyLevel, 0, false,  enableFairQ, enableDeadlockDetection)).target;
            _queueBackPressure = ZeroOnCascade(new IoZeroSemaphoreSlim(AsyncTasks, $"{GetType().Name}: {nameof(_queueBackPressure)}", concurrencyLevel, concurrencyLevel, false, enableFairQ, enableDeadlockDetection)).target;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Shared heap
        /// </summary>
        public ArrayPool<ValueTuple<IIoZero, IMessage, object, Packet>> ArrayPoolProxy { get; protected set; }

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        protected ConcurrentQueue<ValueTuple<IIoZero, IMessage, object, Packet>[]> MessageQueue;

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
        public override string Key => $"{nameof(CcProtocolBuffer)}({Upstream.Key})";
        
        /// <summary>
        /// A description
        /// </summary>
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
        public async Task<bool> EnqueueAsync(ValueTuple<IIoZero, IMessage, object, Packet>[] item)
        {
            var backed = false;
            try
            {
                var backPressure = await _queueBackPressure.WaitAsync().ConfigureAwait(false);
                
                backed = true;
                
                if (!backPressure)
                    return false;

                MessageQueue.Enqueue(item);

                return _queuePressure.Release() != -1;
            }
            catch(Exception e)
            {
                if(backed)
                    _queueBackPressure.Release();

                _logger.Fatal(e, $"{nameof(EnqueueAsync)}: [FAILED], {MessageQueue.Count}, {_queuePressure}");
                return false;
            }
        }


        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public async Task<ValueTuple<IIoZero, IMessage, object, Packet>[]> DequeueAsync()
        {
            try
            {
                ValueTuple<IIoZero, IMessage, object, Packet>[] batch = null;
                while (!Zeroed() && !MessageQueue.TryDequeue(out batch))
                {
                    var checkQ = await _queuePressure.WaitAsync().ConfigureAwait(false);
                    if (Zeroed() || _queueBackPressure.Release() < 0 || !checkQ)
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
