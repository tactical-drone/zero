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
                        var oldTxCutOffValue = new DateTimeOffset(DateTime.Now - transactionArbiter.PrimaryProducer.Upstream.RecentlyProcessed.DupCheckWindow).ToUnixTimeSeconds(); //TODO update to allow older tx if we are not in sync or we requested this tx etc.                            
                        if((transaction.AttachmentTimestamp > 0 && transaction.AttachmentTimestamp < oldTxCutOffValue || transaction.Timestamp < oldTxCutOffValue)
                            && await dataSource.TransactionExistsAsync(transaction.Hash))
                        {
                            stopwatch.Stop();
                            _logger.Warn( $"Slow duplicate tx dropped: [{transaction.AsTrytes(transaction.HashBuffer)}], t = `{stopwatch.ElapsedMilliseconds}ms', T = `{DateTimeOffset.FromUnixTimeSeconds(transaction.Timestamp)}'");
                            transactions.ProcessState = IoProducible<IoTangleTransaction<TBlob>>.State.SlowDup;
                            continue;
                        }

                        // Update milestone mechanics
                        await UpdateMilestoneIndexAsync(dataSource, transaction);

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
        private async Task UpdateMilestoneIndexAsync(IIoDataSource<RowSet> dataSource, IIoTransactionModel<TBlob> transaction)
        {
            var node = (TangleNode<IoTangleMessage<TBlob>, TBlob>)_node;
            transaction.MilestoneIndexEstimate = -1;

            //Update latest seen milestone transaction
            if (transaction.AddressBuffer.Length != 0)
            {                
                if (node.MilestoneTransaction == null && transaction.AsTrytes(transaction.AddressBuffer) ==
                    node.parm_coo_address)
                {
                    node.MilestoneTransaction = transaction;
                    transaction.IsMilestoneTransaction = true;
                    var timeDiff = TimeSpan.FromSeconds(((DateTimeOffset)DateTime.Now).ToUnixTimeSeconds() - transaction.Timestamp);
                    _logger.Debug(IoEntangled<TBlob>.Optimized
                        ? $"New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.AsTrytes(transaction.HashBuffer)}]"
                        : $"New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.Hash}]");
                }
                // ReSharper disable once PossibleNullReferenceException
                else if (node.MilestoneTransaction != null && transaction.AddressBuffer.AsArray().SequenceEqual(node.MilestoneTransaction.AddressBuffer.AsArray()))
                {
                    node.MilestoneTransaction = transaction;
                    transaction.IsMilestoneTransaction = true;
                    var timeDiff = TimeSpan.FromSeconds(((DateTimeOffset)DateTime.Now).ToUnixTimeSeconds() - transaction.Timestamp);
                    _logger.Debug(IoEntangled<TBlob>.Optimized
                        ? $"New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.AsTrytes(transaction.HashBuffer)}]"
                        : $"New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.Hash}]");
                }
            }

            //set transaction milestone
            if (node.MilestoneTransaction != null && node.MilestoneTransaction.Timestamp <= transaction.Timestamp )
            {
                transaction.MilestoneIndexEstimate = node.MilestoneTransaction.GetMilestoneIndex() + 1;
            }
            else //look for a candidate milestone in storage
            {
                var closestMilestone = await ((IoTangleCassandraDb<TBlob>) dataSource).GetClosestMilestone(transaction.Timestamp);                                 
                if (closestMilestone != null)
                {
                    var milestoneTransactionBundle = await ((IoTangleCassandraDb<TBlob>)dataSource).GetAsync(closestMilestone.Bundle);

                    if (node.MilestoneTransaction == null)
                    {
                        node.MilestoneTransaction = milestoneTransactionBundle;
                        var timeDiff = TimeSpan.FromSeconds(((DateTimeOffset) DateTime.Now).ToUnixTimeSeconds() - milestoneTransactionBundle.Timestamp);
                        _logger.Debug(IoEntangled<TBlob>.Optimized
                            ? $"Old milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{milestoneTransactionBundle.AsTrytes(milestoneTransactionBundle.HashBuffer)}]"
                            : $"Old milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{milestoneTransactionBundle.Hash}]");
                    }
                    
                    transaction.MilestoneIndexEstimate = milestoneTransactionBundle.GetMilestoneIndex() + 1;
                 }                     
            }
            //set nearby milestone            
        }
    }
}
