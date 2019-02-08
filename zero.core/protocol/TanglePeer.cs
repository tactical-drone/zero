using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.core;
using zero.core.data.contracts;
using zero.core.data.providers.cassandra.keyspaces.tangle;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.interop.entangled;
using zero.interop.utils;
using Logger = NLog.Logger;

namespace zero.core.protocol
{
    /// <inheritdoc />
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer<TBlob> : IoNeighbor<IoTangleMessage<TBlob>> 
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient<IoTangleMessage<TBlob>> ioNetClient) :
            base($"{nameof(TanglePeer<TBlob>)}",ioNetClient, (userData) => new IoTangleMessage<TBlob>($"rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();            
            //JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads = 2);                        
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Minimum difficulty
        /// </summary>
        public const int MWM = IoPow<TBlob>.MWM;

        /// <summary>
        /// Tcp read ahead
        /// </summary>
        public const int TcpReadAhead = 50;

        /// <summary>
        /// Start processors for this neighbor
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <param name="spawnProducer">Spawns a producer thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(CancellationToken cancellationToken, bool spawnProducer = true)
        {
            var processing = base.SpawnProcessingAsync(cancellationToken, spawnProducer);            
            var persisting = PersistTransactionsAsync(await IoTangleCassandraDb<TBlob>.Default());

            await Task.WhenAll(processing, persisting);
        }

        /// <summary>
        /// Persists transactions seen from this neighbor
        /// </summary>
        /// <typeparam name="TBlob"></typeparam>
        /// <param name="dataSource">An interface to the data source</param>
        /// <returns></returns>
        private async Task PersistTransactionsAsync(IIoDataSource<RowSet> dataSource)
        {
            var relaySource = PrimaryProducer.CreateDownstreamArbiter<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));

            _logger.Debug($"Starting persistence for `{PrimaryProducerDescription}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (relaySource == null)
                {
                    _logger.Warn("Waiting for transaction stream to spin up...");
                    relaySource = PrimaryProducer.CreateDownstreamArbiter<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await relaySource.ConsumeAsync(async batch =>
                {
                    try
                    {
                        await LoadTransactionsAsync(dataSource, batch, relaySource);
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoProducible<IoTangleTransaction<TBlob>>.State.Consumed)
                            batch.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.ConsumeErr;
                    }
                });

                if (!relaySource.PrimaryProducer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{PrimaryProducerDescription}'");
        }

        private async Task LoadTransactionsAsync(IIoDataSource<RowSet> dataSource, IoConsumable<IoTangleTransaction<TBlob>> batch, IoForward<IoTangleTransaction<TBlob>> relaySource)
        {
            
                if (batch == null)
                    return;

                foreach (var transaction in ((IoTangleTransaction<TBlob>) batch).Transactions)
                {
                    var stopwatch = Stopwatch.StartNew();

                    RowSet putResult = null;
                    try
                    {
                        if (await dataSource.TransactionExistsAsync(transaction.Hash))
                        {
                            stopwatch.Stop();
                            _logger.Trace(
                                $"Duplicate tx slow dropped: [{transaction.AsTrytes(transaction.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms'");
                            batch.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.SlowDup;
                            continue;
                        }

                        putResult = await dataSource.PutAsync(transaction);
                        relaySource.IsArbitrating = true;
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e, $"`{nameof(dataSource.PutAsync)}' should never throw exceptions. BUG!");
                        relaySource.IsArbitrating = false;
                    }
                    finally
                    {
                        if (putResult == null)
                        {
                            relaySource.IsArbitrating = false;
                            batch.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.DbError;
                            await relaySource.PrimaryProducer.Upstream.RecentlyProcessed.DeleteKeyAsync(
                                transaction.AsTrytes(transaction.HashBuffer));
                        }
                    }
                }

                batch.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.Consumed;            
        }
    }
}
