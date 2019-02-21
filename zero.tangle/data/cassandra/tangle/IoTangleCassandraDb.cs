using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using NLog;
using zero.core.data.contracts;
using zero.core.data.market;
using zero.core.data.providers.cassandra;
using zero.core.misc;
using zero.core.models;
using zero.core.network.ip;
using zero.tangle.data.cassandra.tangle.luts;
using zero.tangle.entangled;
using zero.tangle.entangled.common.model;
using zero.tangle.entangled.common.model.native;
using zero.tangle.utils;
using Logger = NLog.Logger;

namespace zero.tangle.data.cassandra.tangle
{
    /// <summary>
    /// Tangle Cassandra storage provider
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class IoTangleCassandraDb<TKey> : IoCassandraBase<TKey>, IIoDataSource<RowSet> 
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoTangleCassandraDb():base(new IoTangleKeySpace<TKey>(Entangled<TKey>.Optimized ? "zero" : "one"))
        {            
            _logger = LogManager.GetCurrentClassLogger();            
            _ioTangleKeySpace = (IoTangleKeySpace<TKey>) _keySpaceConfiguration;
        }

        private readonly Logger _logger;

        private readonly IoTangleKeySpace<TKey> _ioTangleKeySpace;
        private Table<IIoTransactionModel<TKey>> _bundles;
        private Table<IoBundledHash<TKey>> _transactions;
        private Table<IoBundledAddress<TKey>> _addresses;
        private Table<IoTaggedTransaction<TKey>> _tags;
        private Table<IoApprovedTransaction<TKey>> _approvees;
        private Table<IoDraggedTransaction<TKey>> _dragnet;
        private Table<IoMilestoneTransaction<TKey>> _milestones;
                            
        /// Partitioners        
        private readonly IoTaggedTransaction<TKey> _tagsPartitioner = new IoTaggedTransaction<TKey>();
        private IoApprovedTransaction<TKey> _approveePartitioner = new IoApprovedTransaction<TKey>();        
        private IoMilestoneTransaction<TKey> _milestonePartitioner = new IoMilestoneTransaction<TKey>();

        private PreparedStatement _dupCheckQuery;
        private string _getTransactionQuery;
        private string _getLowerMilestoneQuery;
        private string _getHigherMilestoneQuery;
        private string _getMilestoneLessTransactions;
        private string _getMilestoneTransactions;
        private PreparedStatement _relaxZeroTransactionMilestoneEstimate;

        /// <summary>
        /// Makes sure that the schema is configured
        /// </summary>
        /// <returns>True if the schema was re-configured, false otherwise</returns>
        protected override async Task<bool> EnsureSchemaAsync()
        {   
            _logger.Debug("Ensuring db schema...");
            IsConfigured = false;

            var wasConfigured = true;

            //ensure keyspace
            var replicationConfig = new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"}, {"replication_factor", "1"}
            };

            _session.CreateKeyspaceIfNotExists(_keySpaceConfiguration.Name, replicationConfig, false);
            _session.ChangeKeyspace(_keySpaceConfiguration.Name);

            //ensure tables                        
            try
            {
                KeyspaceMetadata keyspace = _cluster.Metadata.GetKeyspace(_keySpaceConfiguration.Name);

                while (keyspace == null) 
                {
                    keyspace = _cluster.Metadata.GetKeyspace(_keySpaceConfiguration.Name);
                    await Task.Delay(1000);//TODO config
                }

                var existingTables = keyspace.GetTablesNames();
                
                _bundles = new Table<IIoTransactionModel<TKey>>(_session, _ioTangleKeySpace.BundleMap);
                if (!existingTables.Contains(_bundles.Name))
                {
                    _bundles.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_bundles.Name}'");
                    wasConfigured = false;
                }
                
                                                                    
                _transactions = new Table<IoBundledHash<TKey>>(_session, _ioTangleKeySpace.BundledTransactionMap);
                if (!existingTables.Contains(_transactions.Name))
                {
                    _transactions.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_transactions.Name}'");
                    wasConfigured = false;
                }
                    
                _addresses = new Table<IoBundledAddress<TKey>>(_session, _ioTangleKeySpace.BundledAddressMap);
                if (!existingTables.Contains(_addresses.Name))
                {
                    _addresses.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_addresses.Name}'");
                    wasConfigured = false;
                }
                
                _tags = new Table<IoTaggedTransaction<TKey>>(_session, _ioTangleKeySpace.TaggedTransactionMap);
                if (!existingTables.Contains(_tags.Name))
                {
                    _tags.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_tags.Name}'");
                    wasConfigured = false;
                }

                _approvees = new Table<IoApprovedTransaction<TKey>>(_session, _ioTangleKeySpace.ApprovedTransactionMap);
                if (!existingTables.Contains(_approvees.Name))
                {
                    _approvees.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_approvees.Name}'");
                    wasConfigured = false;
                }

                _dragnet = new Table<IoDraggedTransaction<TKey>>(_session, _ioTangleKeySpace.DraggedTransactionMap);
                if (!existingTables.Contains(_dragnet.Name))
                {
                    _dragnet.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_dragnet.Name}'");
                    wasConfigured = false;
                }

                _milestones = new Table<IoMilestoneTransaction<TKey>>(_session, _ioTangleKeySpace.MilestoneTransactionMap);
                if (!existingTables.Contains(_milestones.Name))
                {
                    _milestones.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_milestones.Name}'");
                    wasConfigured = false;
                }

                //Prepare all queries
                PrepareStatements();                                
            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to ensure schema:");
                return true;                
            }

            _logger.Trace("Ensured schema!");

            //Wait for the config to take hold //TODO hack
            if(wasConfigured)
                await Task.Delay(2000);

            IsConfigured = true;            

            return wasConfigured;
        }

        private void PrepareStatements()
        {
            //check transaction existence 
            _dupCheckQuery = _session.Prepare($"select count(*) from {_transactions.Name} where {nameof(IoBundledHash<TKey>.Hash)}=? limit 1");

            //get transaction by bundle
            //_getTransactionQuery = $"select * from {_bundles.Name} where {nameof(IIoTransactionModel<TKey>.Bundle)}=? order by {nameof(IIoTransactionModel<TKey>.CurrentIndex)} limit 1 allow filtering";
            _getTransactionQuery = $"select * from {_bundles.Name} where {nameof(IIoTransactionModel<TKey>.Bundle)}=? limit 1";

            //get milestone based on upper bound timestamp
            _getLowerMilestoneQuery = $"select * from {_milestones.Name} where {nameof(IoMilestoneTransaction<TKey>.Partition)} in ? and {nameof(IoMilestoneTransaction<TKey>.Timestamp)} <= ? limit 1";

            //relax a milestone based on lower bound timestamp
            _getHigherMilestoneQuery = $"select * from {_milestones.Name} where {nameof(IoMilestoneTransaction<TKey>.Partition)} in ? and {nameof(IoMilestoneTransaction<TKey>.Timestamp)} > ? order by timestamp ASC limit 2";


            //Find transactions without milestone estimates
            _getMilestoneLessTransactions = $"select * from {_approvees.Name} where {nameof(IoApprovedTransaction<TKey>.Partition)} in ? and {nameof(IoApprovedTransaction<TKey>.MilestoneIndexEstimate)} = 0 allow filtering";

            //Find transactions without milestone estimates
            //_getMilestoneTransactions = $"select * from {_approvees.Name} where {nameof(IoApprovedTransaction<TKey>.Partition)} in ? and {nameof(IoApprovedTransaction<TKey>.MilestoneIndexEstimate)} < 1 allow filtering";
            _getMilestoneTransactions = $"select * from {_approvees.Name} where {nameof(IoApprovedTransaction<TKey>.Partition)} in ?";

            //relax a transaction zero milestone
            //_relaxZeroTransactionMilestoneEstimate = _session.Prepare($"update {_approvees.Name} set {nameof(IoApprovedTransaction<TKey>.MilestoneIndexEstimate)}=?, {nameof(IoApprovedTransaction<TKey>.SecondsToMilestone)}=? where {nameof(IoApprovedTransaction<TKey>.Partition)} in ? and  {nameof(IoApprovedTransaction<TKey>.Timestamp)} = ? and {nameof(IoApprovedTransaction<TKey>.Hash)} = ?");
            _relaxZeroTransactionMilestoneEstimate = _session.Prepare($"update {_approvees.Name} " +
                                                                      $"set {nameof(IoApprovedTransaction<TKey>.MilestoneIndexEstimate)}=?, " +
                                                                      $"{nameof(IoApprovedTransaction<TKey>.ConfirmationTime)}=? , " +
                                                                      $"{nameof(IoApprovedTransaction<TKey>.Pow)}=? , " +
                                                                      $"{nameof(IoApprovedTransaction<TKey>.Verifier)}=? , " + //TODO why do I need to spec these?
                                                                      $"{nameof(IoApprovedTransaction<TKey>.IsMilestone)}=? , " +
                                                                      $"{nameof(IoApprovedTransaction<TKey>.Depth)}=? " +
                                                                      $"where {nameof(IoApprovedTransaction<TKey>.Partition)} = ? " +
                                                                        $"and  {nameof(IoApprovedTransaction<TKey>.Timestamp)} = ? " +
                                                                        $"and {nameof(IoApprovedTransaction<TKey>.Hash)} = ?");
        }

        /// <summary>
        /// Puts data into cassandra
        /// </summary>
        /// <param name="transaction">The transaction to persist</param>
        /// <param name="userData">A batch handler</param>
        /// <returns>The rowset with insert results</returns>
        public async Task<RowSet> PutAsync<TTransaction>(TTransaction transaction, object userData = null)
        where TTransaction: class, IIoTransactionModelInterface
        {
            if (!IsConfigured)
                return null;

            var tangleTransaction = (IIoTransactionModel<TKey>)transaction;

            //Basic TX validation check
            if (tangleTransaction.HashBuffer.Length == 0)
            {
                try
                {
                    _logger.Trace($"Invalid transaction");
                    _logger.Trace($"value = `{tangleTransaction.Value}'");
                    _logger.Trace($"pow = `{tangleTransaction.Pow}'");
                    _logger.Trace($"time = `{tangleTransaction.Timestamp}'");
                    _logger.Trace($"bundle = `{tangleTransaction.AsTrytes(tangleTransaction.BundleBuffer)}'");
                    _logger.Trace($"address = `{tangleTransaction.AsTrytes(tangleTransaction.AddressBuffer)}'");
                }
                catch
                {
                    // ignored
                }

                return null;
            }
                                        
            var executeBatch = userData == null;

            var bundledHash = new IoBundledHash<TKey>
            {
                Hash = tangleTransaction.Hash,
                Bundle = tangleTransaction.Bundle,
                Timestamp = tangleTransaction.GetAttachmentTime(),                
            };

            var bundledAddress = new IoBundledAddress<TKey>
            {
                Address = tangleTransaction.Address,
                Bundle = tangleTransaction.Bundle,
                Timestamp = tangleTransaction.GetAttachmentTime()
            };
            
            var taggedTransaction = new IoTaggedTransaction<TKey>
            {
                Tag = tangleTransaction.Tag,
                Partition = tangleTransaction.GetAttachmentTime(),
                Hash = tangleTransaction.Hash,
                Bundle = tangleTransaction.Bundle,
                Timestamp = tangleTransaction.GetAttachmentTime(),
            };

            var verifiedBranchTransaction = new IoApprovedTransaction<TKey>
            {
                Partition = tangleTransaction.GetAttachmentTime(),
                Hash = tangleTransaction.Branch,
                Pow = tangleTransaction.Pow,
                Verifier = tangleTransaction.Hash,
                TrunkBranch = tangleTransaction.Trunk,
                Balance = 0,
                Timestamp = tangleTransaction.GetAttachmentTime(),
                ConfirmationTime = tangleTransaction.ConfirmationTime,
                MilestoneIndexEstimate = tangleTransaction.IsMilestoneTransaction ? tangleTransaction.MilestoneIndexEstimate : -tangleTransaction.MilestoneIndexEstimate,
                IsMilestone = tangleTransaction.IsMilestoneTransaction,
                Depth = tangleTransaction.IsMilestoneTransaction ? 0 : long.MaxValue                
            };

            var verifiedTrunkTransaction = new IoApprovedTransaction<TKey>
            {
                Partition = tangleTransaction.GetAttachmentTime(),
                Hash = tangleTransaction.Trunk,
                Pow = tangleTransaction.Pow,
                Verifier = tangleTransaction.Hash,
                TrunkBranch = tangleTransaction.Branch,
                Balance = 0,
                Timestamp = tangleTransaction.GetAttachmentTime(),
                ConfirmationTime = tangleTransaction.ConfirmationTime,
                MilestoneIndexEstimate = tangleTransaction.IsMilestoneTransaction ? tangleTransaction.MilestoneIndexEstimate : -tangleTransaction.MilestoneIndexEstimate,
                IsMilestone = tangleTransaction.IsMilestoneTransaction,
                Depth = tangleTransaction.IsMilestoneTransaction ? 0 : long.MaxValue                
            };

            var milestoneTransaction = new IoMilestoneTransaction<TKey>
            {                
                Partition = tangleTransaction.GetAttachmentTime(),
                ObsoleteTag = tangleTransaction.ObsoleteTag,
                Hash = tangleTransaction.Hash,
                Bundle = tangleTransaction.Bundle,
                Timestamp = tangleTransaction.GetAttachmentTime(),                
                MilestoneIndex = tangleTransaction.MilestoneIndexEstimate
            };

            if (executeBatch)
                userData = new BatchStatement();

            var batchStatement = ((BatchStatement) userData);

            if (tangleTransaction.Value != 0)
            {                
                try
                {
                    double quality;
                    try
                    {
                        quality = IoMarketDataClient.Quality + (DateTime.Now - (tangleTransaction.Timestamp.ToString().Length > 11? DateTimeOffset.FromUnixTimeMilliseconds(tangleTransaction.Timestamp): DateTimeOffset.FromUnixTimeSeconds(tangleTransaction.Timestamp))).TotalMinutes;

                        if (quality > short.MaxValue)
                            quality = short.MaxValue;
                        if (quality < short.MinValue)
                            quality = short.MinValue;
                    }
                    catch 
                    {
                        quality = short.MaxValue;
                    }

                    var draggedTransaction = new IoDraggedTransaction<TKey>
                    {
                        Address = tangleTransaction.Address,
                        Bundle = tangleTransaction.Bundle,
                        AttachmentTimestamp = tangleTransaction.AttachmentTimestamp,
                        Timestamp = tangleTransaction.Timestamp,
                        LocalTimestamp = ((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds(),
                        Value = tangleTransaction.Value,
                        Direction = (short)(tangleTransaction.CurrentIndex == tangleTransaction.LastIndex? 0:(tangleTransaction.Value>0?1:-1)),
                        Quality = (short)quality,
                        Uri = tangleTransaction.Uri,                                                
                        BtcValue = (float)(tangleTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Btc.Price / IoMarketDataClient.BundleSize)),
                        EthValue = (float)(tangleTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eth.Price / IoMarketDataClient.BundleSize)),
                        EurValue = (float)(tangleTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eur.Price / IoMarketDataClient.BundleSize)),
                        UsdValue = (float)(tangleTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Usd.Price / IoMarketDataClient.BundleSize))
                    };
                    
                    batchStatement.Add(_dragnet.Insert(draggedTransaction as IoDraggedTransaction<TKey>));
                }
                catch (Exception e)
                {
                    _logger.Warn(e, "Unable to drag transaction:");
                }                
            }

            batchStatement.Add(_bundles.Insert((IIoTransactionModel<TKey>) tangleTransaction));
            batchStatement.Add(_transactions.Insert(bundledHash as IoBundledHash<TKey>));
                        
            if(tangleTransaction.BranchBuffer.Length != 0)
                batchStatement.Add(_approvees.Insert(verifiedBranchTransaction as IoApprovedTransaction<TKey>));
            
            if (tangleTransaction.TrunkBuffer.Length != 0)
                batchStatement.Add(_approvees.Insert(verifiedTrunkTransaction as IoApprovedTransaction<TKey>));
            
            if (tangleTransaction.AddressBuffer.Length != 0)
                batchStatement.Add(_addresses.Insert(bundledAddress as IoBundledAddress<TKey>));
            
            if (tangleTransaction.TagBuffer.Length != 0 || tangleTransaction.IsMilestoneTransaction)
                batchStatement.Add(_tags.Insert(taggedTransaction as IoTaggedTransaction<TKey>));

            if (tangleTransaction.IsMilestoneTransaction)
                batchStatement.Add(_milestones.Insert(milestoneTransaction as IoMilestoneTransaction<TKey>));
                        
            if (executeBatch)
            {                
                return await ExecuteAsync(batchStatement);
            }

            return null;
        }

        /// <summary>
        /// Get a transaction from storage
        /// </summary>
        /// <param name="key">The transaction key</param>
        /// <returns>A transaction</returns>
        public async Task<TTransaction> GetAsync<TTransaction, TKeyF>(TKeyF key)
                    where TTransaction: class, IIoTransactionModelInterface
        {
            if (!IsConfigured)
                return default(TTransaction);
            
            if(Entangled<TKey>.Optimized)
                return await Mapper(async (mapper, query, args) => await mapper.FirstOrDefaultAsync<EntangledTransaction>(query, args),_getTransactionQuery, key) as TTransaction;
            else
                return await Mapper(async (mapper, query, args) => await mapper.FirstOrDefaultAsync<TangleNetTransaction>(query, args),_getTransactionQuery, key) as TTransaction;
        }

        /// <summary>
        /// Checks whether a transaction has been loaded
        /// </summary>
        /// <typeparam name="TKeyFunc">The transaction key type</typeparam>
        /// <param name="key">The transaction key</param>
        /// <returns>True if the key was found, false otherwise</returns>
        public async Task<bool> TransactionExistsAsync<TKeyFunc>(TKeyFunc key)
        {
            if (!IsConfigured)
                return false;

            var dupCheckStatement = _dupCheckQuery.Bind(key);
            
            var rowSet = await base.ExecuteAsync(dupCheckStatement);
            if (rowSet != null)
            {
                foreach (var row in rowSet)
                {
                    return row.GetValue<long>("count") > 0;
                }
            }
            
            return false;
        }
        
        /// <summary>
        /// Search for the latest milestone seen before a certain time
        /// </summary>
        /// <param name="timestamp">The nearby timestamp</param>
        /// <returns>The nearest milestone transaction</returns>
        public async Task<IoMilestoneTransaction<TKey>> GetMilestone(long timestamp)
        {
            if (!IsConfigured)
                return null;            
            
            return await Mapper(async (mapper, query, args) => await mapper.FirstOrDefaultAsync<IoMilestoneTransaction<TKey>>(query, args), _getLowerMilestoneQuery, _tagsPartitioner.GetPartitionSet(timestamp), timestamp);
        }

        /// <summary>
        /// Search for the best confirming milestone worse case estimate
        /// </summary>
        /// <param name="timestamp">A nearby timestamp</param>
        /// <returns>A milestone that should confirm this timestamp</returns>
        public async Task<IoMilestoneTransaction<TKey>> GetBestMilestoneEstimate(long timestamp)
        {
            if (!IsConfigured)
                return null;

            var lowerMilestone = await GetMilestone(timestamp);
            var queryTimestamp = lowerMilestone?.Timestamp ?? timestamp;

            var milestoneEstimates = await Mapper(async (mapper, query, args) =>
                await mapper.FetchPageAsync<IoMilestoneTransaction<TKey>>(
                    Cql.New(query, args).WithOptions(options => options.SetPageSize(int.MaxValue))), _getHigherMilestoneQuery, _tagsPartitioner.GetPartitionSet(queryTimestamp), queryTimestamp);

            return milestoneEstimates.LastOrDefault() ?? lowerMilestone;
        }

        /// <summary>
        /// Search for the best confirming milestone worse case estimate
        /// </summary>
        /// <param name="timestamp">A nearby timestamp</param>
        /// <returns>A milestone that should confirm this timestamp</returns>
        public async Task<IIoTransactionModel<TKey>> GetBestMilestoneEstimateBundle(long timestamp)
        {
            //Search a milestone tx
            var bestMilestone = await GetBestMilestoneEstimate(timestamp);
            if (bestMilestone == null) return null;

            //Search the bundle tx of this milestone
            var milestoneBundle = await GetAsync<IIoTransactionModel<TKey>, TKey>(bestMilestone.Bundle);
            if (milestoneBundle == null)
            {
                _logger.Fatal($"Could not find best milestone tx `{bestMilestone.Hash}', bundle = `{bestMilestone.Bundle}' for t = `{timestamp}'");
            }

            //return the milestone bundle tx
            return milestoneBundle;
        }

        /// <summary>
        /// Relaxes zero milestone transaction estimates
        /// </summary>
        /// <param name="milestoneTransaction"></param>
        /// <returns></returns>
        public async Task RelaxZeroTransactionMilestoneEstimates(IIoTransactionModel<TKey> milestoneTransaction)
        {
            var stopwatch = Stopwatch.StartNew();
            var milestoneLessTransactions = (await Mapper(async (mapper, query, args) => await mapper.FetchAsync<IoApprovedTransaction<TKey>>(query, args), _getMilestoneLessTransactions, _approveePartitioner.GetPartitionSet(milestoneTransaction.Timestamp))).ToArray();

            var batch = new BatchStatement();
            var batchSize = 0;
            var processedTx = 0;

            foreach (var milestoneLessTransaction in milestoneLessTransactions)
            {
                batch.Add(_relaxZeroTransactionMilestoneEstimate.Bind( -milestoneTransaction.MilestoneIndexEstimate, (long)(milestoneTransaction.GetAttachmentTime().DateTime() - milestoneLessTransaction.Timestamp.DateTime()).TotalSeconds,
                    _approveePartitioner.GetPartitionSet(milestoneTransaction.Timestamp), milestoneLessTransaction.Timestamp, milestoneLessTransaction.Hash));
                
                if (batchSize++ > 100 || ++processedTx == milestoneLessTransactions.Length) //TODO param
                {
                    var rows = await base.ExecuteAsync(batch);
                    stopwatch.Stop();
                    if(rows.Any())
                        _logger.Info($"Relaxed `{rows.GetRows().First().GetValue<long>("count")}' [zero]milestone estimates to `{milestoneTransaction.GetMilestoneIndex()}', t = `{stopwatch.ElapsedMilliseconds:D}ms'");
                    batchSize = 0;                    
                }
            }
        }

        /// <summary>
        /// Relaxes zero milestone transaction estimates
        /// </summary>
        /// <param name="milestoneTransaction"></param>
        /// <param name="milestones"></param>
        /// <returns></returns>
        public async Task<bool> RelaxTransactionMilestoneEstimates(IIoTransactionModel<TKey> milestoneTransaction, Milestone<TKey> milestones)
        {
            var totalTime = Stopwatch.StartNew();
            var scanTime = Stopwatch.StartNew();
            var transactions = (await Mapper(async (mapper, query, args) => await mapper.FetchAsync<IoApprovedTransaction<TKey>>(query, args), _getMilestoneTransactions, _approveePartitioner.GetPartitionSet(milestoneTransaction.GetAttachmentTime())));
            scanTime.Stop();
            var batch = new BatchStatement();
            batch.SetBatchType(BatchType.Unlogged);
                                    
            var batchSize = 0;
            var processedTx = 0;
            var loadedTx = 0;

            var ioApprovedTransactions = transactions?.ToArray();
            if (!ioApprovedTransactions?.Any()??true)
            {
                _logger.Trace($"No transactions found to relax at milestone = `{milestoneTransaction.GetAttachmentTime().DateTime()}'");
                return false;
            }
            
            var relaxedTransactions = milestones.Relax(ioApprovedTransactions, milestoneTransaction);            

            var loadTime = Stopwatch.StartNew();
            foreach (var milestoneLessTransaction in relaxedTransactions)
            {
                processedTx++;

                batch.Add(_relaxZeroTransactionMilestoneEstimate.Bind(milestoneTransaction.MilestoneIndexEstimate, milestoneLessTransaction.ConfirmationTime,
                    milestoneLessTransaction.Pow, milestoneLessTransaction.Verifier, milestoneLessTransaction.IsMilestone, milestoneLessTransaction.Depth,
                    _approveePartitioner.GetPartition(milestoneLessTransaction.Timestamp), milestoneLessTransaction.Timestamp, milestoneLessTransaction.Hash));

                if (batchSize++ > 50 || processedTx == relaxedTransactions.Count) //TODO param
                {
                    var rows = await base.ExecuteAsync(batch);
                    loadedTx += batchSize;
                    batchSize = 0;
                    batch = new BatchStatement();
                    batch.SetBatchType(BatchType.Unlogged);
                }
            }
            loadTime.Stop();
            totalTime.Stop();
            var confirmed = ioApprovedTransactions.Where(t => t.MilestoneIndexEstimate > 0).Sum(c=>1);
            var latency = ioApprovedTransactions.Where(t => t.MilestoneIndexEstimate > 0).Average(c => c.ConfirmationTime);
            _logger.Info($"Relaxed `{loadedTx}/{ioApprovedTransactions.Length}' milestones estimates from `{milestoneTransaction.GetMilestoneIndex()}', l = `{latency/60:F} min', cr = `{confirmed*100/ioApprovedTransactions.Length:D}%', scan = `{scanTime.ElapsedMilliseconds:D}ms', [load = `{loadTime.ElapsedMilliseconds:D}ms', `{loadedTx * 1000 / (loadTime.ElapsedMilliseconds+1):D} r/s'], [t = `{totalTime.ElapsedMilliseconds:D}ms', `{loadedTx * 1000 / (totalTime.ElapsedMilliseconds + 1):D} r/s']");
            return relaxedTransactions.Any();
        }
                
        /// <summary>
        /// Execute a batch
        /// </summary>
        /// <param name="usedData">The batch handler</param>
        /// <returns>The rowset with execution results</returns>
        public async Task<RowSet> ExecuteAsync(object usedData)
        {
            return await base.ExecuteAsync((BatchStatement)usedData);
        }
        
        private static volatile IoTangleCassandraDb<TKey> _default;
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>
        public static async Task<IIoDataSource<RowSet>> Default()
        {
            if (_default != null) return _default;
            
            _default = new IoTangleCassandraDb<TKey>();
            await _default.ConnectAndConfigureAsync(IoNodeAddress.Create("tcp://10.0.75.1:9042")); //TODO config
            return _default;            
        }
    }
}
