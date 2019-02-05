using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.data.contracts;
using zero.core.data.providers.cassandra;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled;
using Logger = NLog.Logger;

namespace zero.core.core
{
    /// <inheritdoc />
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class IoNeighbor<TJob> : IoProducerConsumer<TJob>
    where TJob : IIoWorker
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="kind">A description of the neighbor</param>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocMessage">The callback that allocates new message buffer space</param>
        public IoNeighbor(string kind, IoNetClient<TJob> ioNetClient, Func<object, IoConsumable<TJob>> mallocMessage)
            : base($"{kind} `{ioNetClient.Description}'", ioNetClient, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();

            Spinners.Token.Register(() => PrimaryProducer?.Close());
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        private bool _closed = false;

        /// <summary>
        /// Called when this neighbor is closed
        /// </summary>
        public event EventHandler Closed;

        /// <summary>
        /// Close this neighbor
        /// </summary>
        public void Close()
        {
            lock (this)
            {
                if (_closed) return;
                _closed = true;
            }
            
            _logger.Info($"Closing neighbor `{PrimaryProducerDescription}'");

            OnClosed();
            
            Spinners.Cancel();            
        }

        /// <summary>
        /// Emits the closed event
        /// </summary>
        public virtual void OnClosed()
        {
            Closed?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <param name="spawnProducer">Spawns a producer thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(CancellationToken cancellationToken, bool spawnProducer = true)
        {
            var processing = base.SpawnProcessingAsync(cancellationToken, spawnProducer);
            var persisting = IoEntangled<object>.Optimized ? PersistTransactionsAsync<byte[]>(await IoCassandra<byte[]>.Default()) : PersistTransactionsAsync<string>(await IoCassandra<string>.Default());

            await Task.WhenAll(processing, persisting);
        }

        /// <summary>
        /// Persists transactions seen from this neighbor
        /// </summary>
        /// <typeparam name="TBlob"></typeparam>
        /// <param name="dataSource">An interface to the data source</param>
        /// <returns></returns>
        private async Task PersistTransactionsAsync<TBlob>(IIoDataSource<RowSet> dataSource)
        {
            var relaySource = PrimaryProducer.GetRelaySource<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));                       
            
            _logger.Debug($"Starting persistence for `{PrimaryProducerDescription}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (relaySource == null)
                {
                    _logger.Warn("Waiting for transaction stream to spin up...");
                    relaySource = PrimaryProducer.GetRelaySource<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await relaySource.ConsumeAsync(async batch =>
                {
                    try
                    {
                        if (batch == null)
                            return;

                        var stopwatch = new Stopwatch();

                        foreach (var transaction in ((IoTangleTransaction<TBlob>) batch).Transactions)
                        {
                            //Dup check
                            stopwatch.Restart();
                            if (await dataSource.Exists(transaction.Hash))
                            {
                                stopwatch.Stop();
                                _logger.Trace($"Duplicate tx slow dropped: [{transaction.AsTrytes(transaction.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms'");
                                batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.SlowDup;
                                continue;                                
                            }

                            var rows = await dataSource.Put(transaction);
                            if (rows == null)
                                batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.ConInvalid;
                        }
                        batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.Consumed;
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoProduceble<IoTangleTransaction<TBlob>>.State.Consumed)
                            batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.ConsumeErr;
                    }
                });

                if (!relaySource.PrimaryProducer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{PrimaryProducerDescription}'");
        }        
    }
}
