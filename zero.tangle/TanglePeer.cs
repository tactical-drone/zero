using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.conf;
using zero.core.core;
using zero.core.data.contracts;
using zero.core.misc;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.interop.utils;
using zero.tangle.data.cassandra.tangle;
using zero.tangle.entangled;
using zero.tangle.models;
using zero.tangle.utils;
using Logger = NLog.Logger;

namespace zero.tangle
{
    /// <inheritdoc />
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer<TKey> : IoNeighbor<IoTangleMessage<TKey>> 
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="node">The node this peer is connected to</param>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(TangleNode<IoTangleMessage<TKey>, TKey> node, IoNetClient<IoTangleMessage<TKey>> ioNetClient) :
            base(node,ioNetClient, (userData) => new IoTangleMessage<TKey>($"rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();            
            //JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads = 2);                        
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        [IoParameter]
        /// <summary>
        /// The time to wait for milestone transactions to settle
        /// </summary>
        private int parm_relax_start_delay_ms = 10000;

        [IoParameter]
        /// <summary>
        /// The number of retries on no relaxation
        /// </summary>
        private int parm_milestone_profiler_max_retry = 4;

        /// <summary>
        /// Minimum difficulty
        /// </summary>
        public const int MWM = Pow<TKey>.MWM;

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
            var persisting = ProcessTransactionsAsync(await IoTangleCassandraDb<TKey>.Default());

            await Task.WhenAll(processing, persisting);
        }

        /// <summary>
        /// Persists transactions seen from this neighbor
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="dataSource">An interface to the data source</param>
        /// <returns></returns>
        private async Task ProcessTransactionsAsync(IIoDataSource<RowSet> dataSource)
        {
            var transactionArbiter = PrimaryProducer.GetDownstreamArbiter<IoTangleTransaction<TKey>>(nameof(TanglePeer<IoTangleTransaction<TKey>>));

            _logger.Debug($"Starting persistence for `{PrimaryProducerDescription}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (transactionArbiter == null)
                {
                    _logger.Warn("Waiting for transaction stream to spin up...");
                    transactionArbiter = PrimaryProducer.GetDownstreamArbiter<IoTangleTransaction<TKey>>(nameof(TanglePeer<IoTangleTransaction<TKey>>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await transactionArbiter.ConsumeAsync(async batch =>
                {
                    try
                    {
                        await ProcessTransactions(dataSource, batch, transactionArbiter,                        
                        async (transaction, forward, arg) =>
                        {
                            await LoadTransactionsAsync(transaction, dataSource, batch, transactionArbiter);
#pragma warning disable 4014
                            //Process milestone transactions
                            if(transaction.IsMilestoneTransaction)
                                Task.Factory.StartNew(async ()=>
                                {
                                    var startTime = DateTime.Now;                                    
                                    //retry maybe some rootish transactions were still incoming
                                    var retries = 0;
                                    var maxRetries = parm_milestone_profiler_max_retry;
                                    while (retries < maxRetries && !await ((IoTangleCassandraDb<TKey>) dataSource).RelaxTransactionMilestoneEstimates(transaction, ((TangleNode<IoTangleMessage<TKey>, TKey>) _node).Milestones))
                                    {                                                                    
                                        retries++;
                                        Thread.Sleep(parm_relax_start_delay_ms);                                        
                                        if((DateTime.Now - startTime).TotalSeconds > ((TangleNode<IoTangleMessage<TKey>, TKey>)_node).Milestones.AveMilestoneSeconds * 0.9)
                                            break;
                                    }
                                },TaskCreationOptions.LongRunning);
#pragma warning restore 4014
                        });
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoProducible<IoTangleTransaction<TKey>>.State.Consumed)
                            batch.ProcessState = IoProducible<IoTangleTransaction<TKey>>.State.ConsumeErr;
                    }
                });

                if (!transactionArbiter.PrimaryProducer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{PrimaryProducerDescription}'");
        }

        private async Task ProcessTransactions(IIoDataSource<RowSet> dataSource,
            IoConsumable<IoTangleTransaction<TKey>> transactions,
            IoForward<IoTangleTransaction<TKey>> transactionArbiter, 
            Func<IIoTransactionModel<TKey>, IoForward<IoTangleTransaction<TKey>>, IIoDataSource<RowSet>, Task> process)
        {
            if (transactions == null)
                return;

            var stopwatch = Stopwatch.StartNew();
            
            var tangleTransactions = ((IoTangleTransaction<TKey>)transactions).Transactions.ToArray();

            foreach (var transaction in tangleTransactions)
            {
                try
                {
                    await process(transaction, transactionArbiter, dataSource);
                }
                catch (Exception e)
                {
                    _logger.Error(e, "Processing transactions failed: ");
                }
            }

            transactions.ProcessState = IoProducible<IoTangleTransaction<TKey>>.State.Consumed;

            stopwatch.Stop();
            //_logger.Trace($"Processed c = `{tangleTransactions.Length}', t = `{stopwatch.ElapsedMilliseconds:D}'");            
        }

        /// <summary>
        /// Load transactions into a datastore
        /// </summary>
        /// <param name="transaction">Transaction being loaded</param>
        /// <param name="dataSource">The datastore loaded into</param>
        /// <param name="consumer">The consumer used to signal events</param>
        /// <param name="transactionArbiter">The arbiter</param>
        /// <returns></returns>
        private async Task LoadTransactionsAsync(IIoTransactionModel<TKey> transaction, IIoDataSource<RowSet> dataSource, IoConsumable<IoTangleTransaction<TKey>> consumer, IoForward<IoTangleTransaction<TKey>> transactionArbiter)
        {
            var stopwatch = Stopwatch.StartNew();
            RowSet putResult = null;
            try
            {
                //drop duplicates
                var oldTxCutOffValue = new DateTimeOffset(DateTime.Now - transactionArbiter.PrimaryProducer.Upstream.RecentlyProcessed.DupCheckWindow).ToUnixTimeMilliseconds(); //TODO update to allow older tx if we are not in sync or we requested this tx etc.                            
                if (transaction.GetAttachmentTime() < oldTxCutOffValue && await dataSource.TransactionExistsAsync(transaction.Hash))
                {
                    stopwatch.Stop();
                    _logger.Warn($"Slow duplicate tx dropped: [{transaction.AsKeyString(transaction.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms', T = `{transaction.Timestamp.DateTime()}'");
                    consumer.ProcessState = IoProducible<IoTangleTransaction<TKey>>.State.SlowDup;
                    return;
                }

                // Update milestone mechanics
                await ((TangleNode<IoTangleMessage<TKey>, TKey>)_node).Milestones.UpdateIndexAsync((TangleNode<IoTangleMessage<TKey>, TKey>)_node, (IoTangleCassandraDb<TKey>)dataSource, transaction);

                //Load the transaction
                putResult = await dataSource.PutAsync(transaction);

                //indicate that loading is happening
                if (!transactionArbiter.IsArbitrating)
                    transactionArbiter.IsArbitrating = true;
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"`{nameof(dataSource.PutAsync)}' should never throw exceptions. BUG!");
                transactionArbiter.IsArbitrating = false;
            }
            finally
            {
                if (putResult == null && consumer.ProcessState != IoProducible<IoTangleTransaction<TKey>>.State.SlowDup)
                {
                    transactionArbiter.IsArbitrating = false;
                    consumer.ProcessState = IoProducible<IoTangleTransaction<TKey>>.State.DbError;
                    await transactionArbiter.PrimaryProducer.Upstream.RecentlyProcessed.DeleteKeyAsync(
                        transaction.AsTrytes(transaction.HashBuffer));
                }
            }            
        }               
    }
}
