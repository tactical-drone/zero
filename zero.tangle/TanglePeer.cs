using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using NLog;
using zero.core.core;
using zero.core.data.contracts;
using zero.core.misc;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled;
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
    public class TanglePeer<TBlob> : IoNeighbor<IoTangleMessage<TBlob>> 
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="node">The node this peer is connected to</param>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(TangleNode<IoTangleMessage<TBlob>, TBlob> node, IoNetClient<IoTangleMessage<TBlob>> ioNetClient) :
            base(node,ioNetClient, (userData) => new IoTangleMessage<TBlob>($"rx", $"{ioNetClient.AddressString}", ioNetClient))
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
            var transactionArbiter = PrimaryProducer.CreateDownstreamArbiter<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));

            _logger.Debug($"Starting persistence for `{PrimaryProducerDescription}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (transactionArbiter == null)
                {
                    _logger.Warn("Waiting for transaction stream to spin up...");
                    transactionArbiter = PrimaryProducer.CreateDownstreamArbiter<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await transactionArbiter.ConsumeAsync(async batch =>
                {
                    try
                    {
                        await LoadTransactionsAsync(dataSource, batch, transactionArbiter);
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState != IoProducible<IoTangleTransaction<TBlob>>.State.Consumed)
                            batch.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.ConsumeErr;
                    }
                });

                if (!transactionArbiter.PrimaryProducer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{PrimaryProducerDescription}'");
        }

        /// <summary>
        /// Load transactions into a datastore
        /// </summary>
        /// <param name="dataSource">The datastore</param>
        /// <param name="transactions">The transactions to load</param>
        /// <param name="transactionArbiter">The arbiter</param>
        /// <returns></returns>
        private async Task LoadTransactionsAsync(IIoDataSource<RowSet> dataSource, IoConsumable<IoTangleTransaction<TBlob>> transactions, IoForward<IoTangleTransaction<TBlob>> transactionArbiter)
        {
            
                if (transactions == null)
                    return;

                foreach (var transaction in ((IoTangleTransaction<TBlob>) transactions).Transactions)
                {
                    var stopwatch = Stopwatch.StartNew();

                    RowSet putResult = null;
                    try
                    {
                        //drop duplicates
                        var oldTxCutOffValue = new DateTimeOffset(DateTime.Now - transactionArbiter.PrimaryProducer.Upstream.RecentlyProcessed.DupCheckWindow).ToUnixTimeMilliseconds(); //TODO update to allow older tx if we are not in sync or we requested this tx etc.                            
                        if((transaction.AttachmentTimestamp > 0 && transaction.AttachmentTimestamp < oldTxCutOffValue || transaction.Timestamp < oldTxCutOffValue)
                            && await dataSource.TransactionExistsAsync(transaction.Hash))
                        {
                            stopwatch.Stop();
                            _logger.Warn( $"Slow duplicate tx dropped: [{transaction.AsTrytes(transaction.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms', T = `{transaction.Timestamp.DateTime()}'");
                            transactions.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.SlowDup;
                            continue;
                        }

                        // Update milestone mechanics
                        await UpdateMilestoneIndexAsync((IoTangleCassandraDb<TBlob>) dataSource, transaction);

                        //Load the transaction
                        putResult = await dataSource.PutAsync(transaction);

                        //indicate that loading is happening
                        if(!transactionArbiter.IsArbitrating)
                            transactionArbiter.IsArbitrating = true;
                    }
                    catch (Exception e)
                    {
                        _logger.Fatal(e, $"`{nameof(dataSource.PutAsync)}' should never throw exceptions. BUG!");
                        transactionArbiter.IsArbitrating = false;
                    }
                    finally
                    {
                        if (putResult == null && transactions.ProcessState != IoProducible<IoTangleTransaction<TBlob>>.State.SlowDup)
                        {
                            transactionArbiter.IsArbitrating = false;
                            transactions.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.DbError;
                            await transactionArbiter.PrimaryProducer.Upstream.RecentlyProcessed.DeleteKeyAsync(
                                transaction.AsTrytes(transaction.HashBuffer));
                        }
                    }
                }

                transactions.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.Consumed;            
        }

        /// <summary>
        /// Update milestone mechanics
        /// </summary>
        /// <param name="dataSource">The source where milestone data can be found</param>
        /// <param name="transaction">The latest transaction</param>
        /// <returns></returns>
        private async Task UpdateMilestoneIndexAsync(IoTangleCassandraDb<TBlob> dataSource, IIoTransactionModel<TBlob> transaction)
        {
            var node = (TangleNode<IoTangleMessage<TBlob>, TBlob>)_node;
            transaction.MilestoneIndexEstimate = 0;

            //Update latest seen milestone transaction
            if (   node.LatestMilestoneTransaction == null && transaction.AsTrytes(transaction.AddressBuffer) == node.parm_coo_address
                || node.LatestMilestoneTransaction != null && transaction.AddressBuffer.AsArray().SequenceEqual(node.LatestMilestoneTransaction.AddressBuffer.AsArray())
               )
            {
                transaction.SecondsToMilestone = 0;
                transaction.IsMilestoneTransaction = true;
                transaction.MilestoneEstimateTransaction = transaction;
                transaction.MilestoneIndexEstimate = transaction.GetMilestoneIndex();
                
                //relax zero transaction milestone estimates that belong to this milestone
                await dataSource.RelaxZeroTransactionMilestoneEstimates(transaction);

                //relax zero transaction milestone estimates that belong to this milestone
                await dataSource.RelaxTransactionMilestoneEstimates(transaction);

                if (transaction.Timestamp > (node.LatestMilestoneTransaction?.Timestamp??0))
                {
                    node.LatestMilestoneTransaction = transaction;

                    var timeDiff = DateTime.Now - transaction.Timestamp.DateTime();
                    _logger.Info(IoEntangled<TBlob>.Optimized
                        ? $"[{transaction.Timestamp.DateTime()}]: New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.AsTrytes(transaction.HashBuffer)}]"
                        : $"[{transaction.Timestamp.DateTime()}]: New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.Hash}]");
                }                                
            }            
            //Load from the DB if we don't have one ready
            else if (node.LatestMilestoneTransaction == null) 
            {                    
                node.LatestMilestoneTransaction = await dataSource.GetBestMilestoneEstimateBundle(((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds());

                if (node.LatestMilestoneTransaction != null)
                {
                    var timeDiff = DateTime.Now - node.LatestMilestoneTransaction.Timestamp.DateTime();
                    _logger.Debug(IoEntangled<TBlob>.Optimized
                        ? $"Loaded latest milestoneIndex = `{node.LatestMilestoneTransaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{node.LatestMilestoneTransaction.AsTrytes(node.LatestMilestoneTransaction.HashBuffer)}]"
                        : $"Loaded latest milestoneIndex = `{node.LatestMilestoneTransaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{node.LatestMilestoneTransaction.Hash}]");
                }                    
            }

            //If this is a milestone transaction there is nothing more to be done
            if( transaction.IsMilestoneTransaction)
                return;

            //set transaction milestone estimate if the transaction newer than newest milestone seen
            if (node.LatestMilestoneTransaction != null && node.LatestMilestoneTransaction.Timestamp <= transaction.Timestamp )
            {
                transaction.MilestoneIndexEstimate = node.LatestMilestoneTransaction.GetMilestoneIndex() + 2;
                transaction.SecondsToMilestone = 90; //TODO param
            }
            else //look for a candidate milestone in storage for older transactions
            {
                var stopwatch = Stopwatch.StartNew();
                var relaxMilestone = await dataSource.GetBestMilestoneEstimateBundle(transaction.Timestamp);
                stopwatch.Stop();
                
                try
                {                    
                    if (relaxMilestone != null)
                        _logger.Trace($"Relaxed milestone: `{relaxMilestone.MilestoneIndexEstimate = relaxMilestone.GetMilestoneIndex()}', dt = `{relaxMilestone.Timestamp.DateTime().DateTime - transaction.Timestamp.DateTime().DateTime}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                    else
                    {
                        try
                        {
                            _logger.Trace($"Milestone not found: `{transaction.Timestamp}' = `{transaction.Timestamp.DateTime().DateTime}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                        }
                        catch
                        {
                            _logger.Trace($"Milestone not found: `{transaction.Timestamp}', t = `{stopwatch.ElapsedMilliseconds}ms'");                            
                        }
                    }
                }
                catch (Exception e)
                {
                    _logger.Trace($"Cannot find milestone for invalid date: `{transaction.Timestamp.DateTime()}'");                    
                }
                
                transaction.MilestoneIndexEstimate = (relaxMilestone)?.GetMilestoneIndex()??0;

                if(relaxMilestone != null)
                    transaction.SecondsToMilestone = (long) ((transaction.AttachmentTimestamp > 0 ? transaction.AttachmentTimestamp : transaction.Timestamp).DateTime() - relaxMilestone.GetMilestoneIndex().DateTime()).TotalSeconds;
            }            
        }
    }
}
