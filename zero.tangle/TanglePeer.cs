using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.conf;
using zero.core.core;
using zero.core.data.contracts;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.interop.utils;
using zero.tangle.data.cassandra.tangle;
using zero.tangle.models;
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
        /// <param name="spawnProducer">Spawns a source thread</param>
        /// <returns></returns>
        public override async Task SpawnProcessingAsync(bool spawnProducer = true)
        {
            var processing = base.SpawnProcessingAsync(spawnProducer);            
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
            var transactionArbiter = Source.AttachProducer<IoTangleTransaction<TKey>>(nameof(TanglePeer<IoTangleTransaction<TKey>>));

            _logger.Debug($"Starting persistence for `{Description}'");
            while (!Zeroed())
            {
                if (transactionArbiter == null)
                {
                    _logger.Warn("Waiting for transaction stream to spin up...");
                    transactionArbiter = Source.AttachProducer<IoTangleTransaction<TKey>>(nameof(TanglePeer<IoTangleTransaction<TKey>>));
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
                            await LoadTransactionAsync(transaction, dataSource, batch, transactionArbiter);
#pragma warning disable 4014
                            //Process milestone transactions
                            if (transaction.IsMilestoneTransaction)
                            {
                                _logger.Trace($"{batch.TraceDescription} Relaxing tx milestones to [{transaction.AsKeyString(transaction.HashBuffer)}] [THREADSTART]");
                                Task.Factory.StartNew(async () =>
                                {
                                    var startTime = DateTime.Now;
                                    //retry maybe some rootish transactions were still incoming
                                    var retries = 0;
                                    var maxRetries = parm_milestone_profiler_max_retry;
                                    _logger.Trace($"{batch.TraceDescription} Relaxing tx milestones to [{transaction.AsKeyString(transaction.HashBuffer)}] [RETRY {retries}]");
                                    while (retries < maxRetries && !await ((IoTangleCassandraDb<TKey>)dataSource).RelaxTransactionMilestoneEstimates(transaction, ((TangleNode<IoTangleMessage<TKey>, TKey>)Node).Milestones, batch.TraceDescription))
                                    {
                                        retries++;
                                        //Task.Delay(parm_relax_start_delay_ms);
                                        Thread.Sleep(parm_relax_start_delay_ms);
                                        if ((DateTime.Now - startTime).TotalSeconds > ((TangleNode<IoTangleMessage<TKey>, TKey>)Node).Milestones.AveMilestoneSeconds * 0.9)
                                            break;
                                    }
                                }, TaskCreationOptions.LongRunning);
                            }
#pragma warning restore 4014
                        });
                    }
                    finally
                    {
                        if (batch != null && batch.State != IoJob<IoTangleTransaction<TKey>>.JobState.Consumed)
                            batch.State = IoJob<IoTangleTransaction<TKey>>.JobState.ConsumeErr;
                    }
                });

                if (!transactionArbiter.Source.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{Description}'");
        }

        /// <summary>
        /// Processes all transactions
        /// </summary>
        /// <param name="dataSource">A data source used for processing</param>
        /// <param name="transactions">The transactions that need processing</param>
        /// <param name="transactionArbiter">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <returns></returns>
        private async Task ProcessTransactions(IIoDataSource<RowSet> dataSource,
            IoLoad<IoTangleTransaction<TKey>> transactions,
            IoChannel<IoTangleTransaction<TKey>> transactionArbiter, 
            Func<IIoTransactionModel<TKey>, IoChannel<IoTangleTransaction<TKey>>, IIoDataSource<RowSet>, Task> processCallback)
        {
            if (transactions == null)
                return;

            var stopwatch = Stopwatch.StartNew();
            
            var tangleTransactions = ((IoTangleTransaction<TKey>)transactions).Transactions.ToArray();

            _logger.Trace($"{transactions.TraceDescription} Processing `{tangleTransactions.Length}' transactions...");
            foreach (var transaction in tangleTransactions)
            {
                try
                {
                    await processCallback(transaction, transactionArbiter, dataSource);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"{transactions.TraceDescription} Processing transactions failed: ");
                }
            }

            transactions.State = IoJob<IoTangleTransaction<TKey>>.JobState.Consumed;

            stopwatch.Stop();
            _logger.Trace($"{transactions.TraceDescription} Processed `{tangleTransactions.Length}' transactions: t = `{stopwatch.ElapsedMilliseconds:D}', `{tangleTransactions.Length*1000/(stopwatch.ElapsedMilliseconds+1):D} t/s'");
        }

        /// <summary>
        /// Load transactions into a datastore
        /// </summary>
        /// <param name="transaction">Transaction being loaded</param>
        /// <param name="dataSource">The datastore loaded into</param>
        /// <param name="consumer">The consumer used to signal events</param>
        /// <param name="transactionArbiter">The arbiter</param>
        /// <returns></returns>
        private async Task LoadTransactionAsync(IIoTransactionModel<TKey> transaction, IIoDataSource<RowSet> dataSource, IoLoad<IoTangleTransaction<TKey>> consumer, IoChannel<IoTangleTransaction<TKey>> transactionArbiter)
        {
            var stopwatch = Stopwatch.StartNew();
            //_logger.Trace($"{consumer.TraceDescription} Loading transaction [ENTER]");
            RowSet putResult = null;
            try
            {
                //drop duplicates fast if redis is present supported
                //if(transactionArbiter.Source.ChannelSource.RecentlyProcessed != null)
                //{
                //    var oldTxCutOffValue = new DateTimeOffset(DateTime.Now - transactionArbiter.Source.ChannelSource.RecentlyProcessed.DupCheckWindow).ToUnixTimeMilliseconds(); //TODO update to allow older tx if we are not in sync or we requested this tx etc.                            
                //    if (transaction.GetAttachmentTime() < oldTxCutOffValue && await dataSource.TransactionExistsAsync(transaction.Hash))
                //    {
                //        stopwatch.Stop();
                //        _logger.Trace($"{consumer.TraceDescription} Slow duplicate tx dropped: [{transaction.AsKeyString(transaction.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms', T = `{transaction.Timestamp.DateTime()}'");
                //        consumer.State = IoJob<IoTangleTransaction<TKey>>.JobState.SlowDup;
                //        return;
                //    }
                //}
                                
                // Update milestone mechanics
                await ((TangleNode<IoTangleMessage<TKey>, TKey>)Node).Milestones.UpdateIndexAsync((TangleNode<IoTangleMessage<TKey>, TKey>)Node, (IoTangleCassandraDb<TKey>)dataSource, transaction);
                
                //Load the transaction
                putResult = await dataSource.PutAsync(transaction);

                //indicate that loading is happening
                if (!transactionArbiter.IsArbitrating)
                    transactionArbiter.IsArbitrating = true;                
            }
            catch (Exception e)
            {
                _logger.Fatal(e, $"{consumer.TraceDescription} `{nameof(dataSource.PutAsync)}' should never throw exceptions. BUG!");
                transactionArbiter.IsArbitrating = false;
            }
            finally
            {
                if (putResult == null && consumer.State != IoJob<IoTangleTransaction<TKey>>.JobState.SlowDup)
                {
                    transactionArbiter.IsArbitrating = false;
                    consumer.State = IoJob<IoTangleTransaction<TKey>>.JobState.DbError;

                    //if(transactionArbiter.Source.ChannelSource.RecentlyProcessed != null)
                    //    await transactionArbiter.Source.ChannelSource.RecentlyProcessed.DeleteKeyAsync(transaction.AsTrytes(transaction.HashBuffer));
                }                
            }            
        }               
    }
}
