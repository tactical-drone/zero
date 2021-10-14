﻿using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;

namespace zero.core.models.protobuffer.sources
{
    /// <summary>
    /// Used as a source of unmarshalled protobuf msgs by <see cref="IoConduit{TJob}"/> for <see cref="CcAdjunct"/>
    /// </summary>
    public class CcProtocBatchSource<TModel, TBatch> : IoSource<CcProtocBatch<TModel, TBatch>>
    where TModel : IMessage
    where TBatch : IoNanoprobe
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="description">A description</param>
        /// <param name="ioSource">The source of this model</param>
        /// <param name="arrayPool">Used to establish a pool</param>
        /// <param name="batchSize">Batch size</param>
        /// <param name="prefetchSize">Initial job prefetch from source</param>
        /// <param name="concurrencyLevel"></param>
        /// <param name="maxAsyncSinks"></param>
        /// <param name="maxAsyncSources"></param>
        public CcProtocBatchSource(string description, IIoSource ioSource,ArrayPool<TBatch> arrayPool, uint batchSize, int prefetchSize, int concurrencyLevel, uint maxAsyncSinks = 0, uint maxAsyncSources = 0) 
            : base(description, prefetchSize, concurrencyLevel, maxAsyncSinks, maxAsyncSources)//TODO config
        {
            _logger = LogManager.GetCurrentClassLogger();

            Upstream = ioSource;
            ArrayPool = arrayPool;

            //Set Q to be blocking
            //TODO tuning

            MessageQueue = new IoQueue<TBatch[]>($"{nameof(CcProtocBatchSource<TModel,TBatch>)}: {ioSource.Description}", batchSize, concurrencyLevel, true, false);
    
            var enableFairQ = false;
            var enableDeadlockDetection = true;
#if RELEASE
            enableDeadlockDetection = false;
#endif
            
            //TODO tuning
            _queuePressure = new IoZeroSemaphoreSlim(AsyncTasks.Token, $"{GetType().Name}: {nameof(_queuePressure)}",
                maxBlockers: concurrencyLevel, initialCount: 0, maxAsyncWork:0, enableAutoScale: false,  enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);
            
            // _queueBackPressure = new IoZeroSemaphoreSlim(AsyncTasks.Token,  $"{GetType().Name}: {nameof(_queueBackPressure)}", 
            //     maxBlockers: ioSource.ZeroConcurrencyLevel(), initialCount: 1, maxAsyncWork: 0, enableAutoScale: false, enableFairQ: enableFairQ, enableDeadlockDetection: enableDeadlockDetection);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// Shared heap
        /// </summary>
        public ArrayPool<TBatch> ArrayPool { get; protected set; }

        /// <summary>
        /// Used to load the next value to be produced
        /// </summary>
        protected IoQueue<TBatch[]> MessageQueue;

        /// <summary>
        /// Sync used to access the Q
        /// </summary>
        private IoZeroSemaphoreSlim _queuePressure;

        // /// <summary>
        // /// Sync used to access the Q
        // /// </summary>
        // private IoZeroSemaphoreSlim _queueBackPressure;

        /// <summary>
        /// Keys this instance.
        /// </summary>
        public override string Key => $"{nameof(CcProtocBatchSource<TModel, TBatch>)}({Upstream?.Key})";
        
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
        public override bool IsOperational => !Zeroed();

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            _queuePressure = null;
            //_queueBackPressure = null;
            MessageQueue = null;
            ArrayPool = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            _queuePressure.Zero();
            //_queueBackPressure.Zero();
            await MessageQueue.ZeroManagedAsync(static async (msgBatch,@this) =>
            {
                foreach (var msg in msgBatch)
                {
                    if(msg == null)
                        break;
                    
                    await msg.ZeroAsync(@this).FastPath().ConfigureAwait(false);
                }
                    
            },this).FastPath().ConfigureAwait(false);
            
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
        }

        /// <summary>
        /// Enqueue a batch
        /// </summary>
        /// <param name="item">The messages</param>
        /// <returns>Async task</returns>
        public async ValueTask<bool> EnqueueAsync(TBatch[] item)
        {
            try
            {
                // backPressure = _queueBackPressure.WaitAsync();
                // if (!await backPressure.FastPath().ConfigureAwait(false))
                //     return false;

                var plugged = await MessageQueue.EnqueueAsync(item).FastPath().ConfigureAwait(false) != null;

                await _queuePressure.ReleaseAsync().FastPath().ConfigureAwait(false);
                
                return plugged;
            }
            catch (Exception e)
            {
                if (!Zeroed())
                    _logger.Fatal(e, $"{nameof(EnqueueAsync)}: [FAILED], {MessageQueue.Count}, {_queuePressure}");
                return false;
            }
        }


        /// <summary>
        /// Dequeue item
        /// </summary>
        /// <returns></returns>
        public async ValueTask<TBatch[]> DequeueAsync()
        {
            try
            {
                if (!await _queuePressure.WaitAsync().FastPath().ConfigureAwait(false))
                    return null;
                
                return await MessageQueue.DequeueAsync().FastPath().ConfigureAwait(false);
            }
            catch when (Zeroed()){}
            catch (Exception e)when (!Zeroed())
            {
                _logger.Trace(e, $"{Description}");
            }
            finally
            {
                try
                {
                    //_queueBackPressure.Release();
                }
                catch
                {
                    // ignored
                }
            }

            return null;
        }

        /// <summary>
        /// Queue count
        /// </summary>
        /// <returns>returns number of items in the q</returns>
        public uint Count()
        {
            return (uint)MessageQueue.Count;
        }

        /// <summary>
        /// Produces the specified callback.
        /// </summary>
        /// <param name="callback">The callback.</param>
        /// <param name="jobClosure"></param>
        /// <param name="barrier"></param>
        /// <param name="nanite"></param>
        /// <returns>The async task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<bool> ProduceAsync<T>(
            Func<IIoSourceBase, Func<IIoJob, T, ValueTask<bool>>, T, IIoJob, ValueTask<bool>> callback,
            IIoJob jobClosure = null,
            Func<IIoJob, T, ValueTask<bool>> barrier = null,
            T nanite = default)
        {
            try
            {
                return await callback(this, barrier, nanite, jobClosure).FastPath().ConfigureAwait(false);
            }
            catch (Exception) when(!Zeroed()){}
            catch (Exception e)
            {
                _logger.Error(e, $"Source `{Description??"N/A"}' callback failed:");
            }

            return false;
        }
    }
}
