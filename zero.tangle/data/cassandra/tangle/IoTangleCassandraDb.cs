using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
using zero.interop.entangled;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using zero.tangle.data.cassandra.tangle.luts;
using zero.tangle.models;
using Logger = NLog.Logger;

namespace zero.tangle.data.cassandra.tangle
{
    /// <summary>
    /// Tangle Cassandra storage provider
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoTangleCassandraDb<TBlob> : IoCassandraBase<TBlob>, IIoDataSource<RowSet> 
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoTangleCassandraDb():base(new IoTangleKeySpace<TBlob>(IoEntangled<TBlob>.Optimized ? "zero" : "one"))
        {            
            _logger = LogManager.GetCurrentClassLogger();            
            _ioTangleKeySpace = (IoTangleKeySpace<TBlob>) _keySpaceConfiguration;
        }

        private readonly Logger _logger;

        private readonly IoTangleKeySpace<TBlob> _ioTangleKeySpace;
        private Table<IIoTransactionModel<TBlob>> _bundles;
        private Table<IoBundledHash<TBlob>> _transactions;
        private Table<IoBundledAddress<TBlob>> _addresses;
        private Table<IoTaggedTransaction<TBlob>> _tags;
        private Table<IoApprovedTransaction<TBlob>> _approvees;
        private Table<IoDraggedTransaction<TBlob>> _dragnet;
        private Table<IoMilestoneTransaction<TBlob>> _milestones;
                            
        /// Partitioners        
        private readonly IoTaggedTransaction<TBlob> _tagsPartitioner = new IoTaggedTransaction<TBlob>();
        private IoApprovedTransaction<TBlob> _approveePartitioner = new IoApprovedTransaction<TBlob>();        
        private IoMilestoneTransaction<TBlob> _milestonePartitioner = new IoMilestoneTransaction<TBlob>();

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
                
                _bundles = new Table<IIoTransactionModel<TBlob>>(_session, _ioTangleKeySpace.BundleMap);
                if (!existingTables.Contains(_bundles.Name))
                {
                    _bundles.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_bundles.Name}'");
                    wasConfigured = false;
                }
                
                                                                    
                _transactions = new Table<IoBundledHash<TBlob>>(_session, _ioTangleKeySpace.BundledTransactionMap);
                if (!existingTables.Contains(_transactions.Name))
                {
                    _transactions.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_transactions.Name}'");
                    wasConfigured = false;
                }
                    
                _addresses = new Table<IoBundledAddress<TBlob>>(_session, _ioTangleKeySpace.BundledAddressMap);
                if (!existingTables.Contains(_addresses.Name))
                {
                    _addresses.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_addresses.Name}'");
                    wasConfigured = false;
                }
                
                _tags = new Table<IoTaggedTransaction<TBlob>>(_session, _ioTangleKeySpace.TaggedTransactionMap);
                if (!existingTables.Contains(_tags.Name))
                {
                    _tags.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_tags.Name}'");
                    wasConfigured = false;
                }

                _approvees = new Table<IoApprovedTransaction<TBlob>>(_session, _ioTangleKeySpace.ApprovedTransactionMap);
                if (!existingTables.Contains(_approvees.Name))
                {
                    _approvees.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_approvees.Name}'");
                    wasConfigured = false;
                }

                _dragnet = new Table<IoDraggedTransaction<TBlob>>(_session, _ioTangleKeySpace.DraggedTransactionMap);
                if (!existingTables.Contains(_dragnet.Name))
                {
                    _dragnet.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_dragnet.Name}'");
                    wasConfigured = false;
                }

                _milestones = new Table<IoMilestoneTransaction<TBlob>>(_session, _ioTangleKeySpace.MilestoneTransactionMap);
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
            _dupCheckQuery = _session.Prepare($"select count(*) from {_transactions.Name} where {nameof(IoBundledHash<TBlob>.Hash)}=? limit 1");

            //get transaction by bundle
            //_getTransactionQuery = $"select * from {_bundles.Name} where {nameof(IIoTransactionModel<TBlob>.Bundle)}=? order by {nameof(IIoTransactionModel<TBlob>.CurrentIndex)} limit 1 allow filtering";
            _getTransactionQuery = $"select * from {_bundles.Name} where {nameof(IIoTransactionModel<TBlob>.Bundle)}=? limit 1";

            //get milestone based on upper bound timestamp
            _getLowerMilestoneQuery = $"select * from {_milestones.Name} where {nameof(IoMilestoneTransaction<TBlob>.Partition)} in ? and {nameof(IoMilestoneTransaction<TBlob>.Timestamp)} <= ? limit 1";

            //relax a milestone based on lower bound timestamp
            _getHigherMilestoneQuery = $"select * from {_milestones.Name} where {nameof(IoMilestoneTransaction<TBlob>.Partition)} in ? and {nameof(IoMilestoneTransaction<TBlob>.Timestamp)} > ? order by timestamp ASC limit 2";


            //Find transactions without milestone estimates
            _getMilestoneLessTransactions = $"select * from {_approvees.Name} where {nameof(IoApprovedTransaction<TBlob>.Partition)} in ? and {nameof(IoApprovedTransaction<TBlob>.MilestoneIndexEstimate)} = 0 allow filtering";

            //Find transactions without milestone estimates
            _getMilestoneTransactions = $"select * from {_approvees.Name} where {nameof(IoApprovedTransaction<TBlob>.Partition)} in ? and {nameof(IoApprovedTransaction<TBlob>.MilestoneIndexEstimate)} < 1 allow filtering";

            //relax a transaction zero milestone
            //_relaxZeroTransactionMilestoneEstimate = _session.Prepare($"update {_approvees.Name} set {nameof(IoApprovedTransaction<TBlob>.MilestoneIndexEstimate)}=?, {nameof(IoApprovedTransaction<TBlob>.SecondsToMilestone)}=? where {nameof(IoApprovedTransaction<TBlob>.Partition)} in ? and  {nameof(IoApprovedTransaction<TBlob>.Timestamp)} = ? and {nameof(IoApprovedTransaction<TBlob>.Hash)} = ?");
            _relaxZeroTransactionMilestoneEstimate = _session.Prepare($"update {_approvees.Name} set {nameof(IoApprovedTransaction<TBlob>.MilestoneIndexEstimate)}=?, {nameof(IoApprovedTransaction<TBlob>.SecondsToMilestone)}=? , {nameof(IoApprovedTransaction<TBlob>.Pow)}=? , {nameof(IoApprovedTransaction<TBlob>.Verifier)}=? where {nameof(IoApprovedTransaction<TBlob>.Partition)} in ? and  {nameof(IoApprovedTransaction<TBlob>.Timestamp)} = ? and {nameof(IoApprovedTransaction<TBlob>.Hash)} = ?");
        }

        /// <summary>
        /// Puts data into cassandra
        /// </summary>
        /// <param name="transaction">The transaction to persist</param>
        /// <param name="userData">A batch handler</param>
        /// <returns>The rowset with insert results</returns>
        public async Task<RowSet> PutAsync<TBlobLocal>(IIoTransactionModel<TBlobLocal> transaction, object userData = null)
        {
            if (!IsConfigured)
                return null;

            //Basic TX validation check
            if (transaction.HashBuffer.Length == 0)
            {
                try
                {
                    _logger.Trace($"Invalid transaction");
                    _logger.Trace($"value = `{transaction.Value}'");
                    _logger.Trace($"pow = `{transaction.Pow}'");
                    _logger.Trace($"time = `{transaction.Timestamp}'");
                    _logger.Trace($"bundle = `{transaction.AsTrytes(transaction.BundleBuffer)}'");
                    _logger.Trace($"address = `{transaction.AsTrytes(transaction.AddressBuffer)}'");
                }
                catch
                {
                    // ignored
                }

                return null;
            }
                                        
            var executeBatch = userData == null;

            var bundledHash = new IoBundledHash<TBlobLocal>
            {
                Hash = transaction.Hash,
                Bundle = transaction.Bundle,
                Timestamp = transaction.GetAttachmentTime(),                
            };

            var bundledAddress = new IoBundledAddress<TBlobLocal>
            {
                Address = transaction.Address,
                Bundle = transaction.Bundle,
                Timestamp = transaction.GetAttachmentTime()
            };
            
            var taggedTransaction = new IoTaggedTransaction<TBlobLocal>
            {
                Tag = transaction.Tag,
                Partition = transaction.GetAttachmentTime(),
                Hash = transaction.Hash,
                Bundle = transaction.Bundle,
                Timestamp = transaction.GetAttachmentTime(),
            };
            
            var verifiedBranchTransaction = new IoApprovedTransaction<TBlobLocal>
            {
                Partition = transaction.GetAttachmentTime(),
                Hash = transaction.Branch,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,
                Timestamp = transaction.GetAttachmentTime(),
                SecondsToMilestone = transaction.SecondsToMilestone,
                MilestoneIndexEstimate = transaction.IsMilestoneTransaction ? transaction.MilestoneIndexEstimate : -transaction.MilestoneIndexEstimate
            };

            var verifiedTrunkTransaction = new IoApprovedTransaction<TBlobLocal>
            {
                Partition = transaction.GetAttachmentTime(),
                Hash = transaction.Trunk,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,
                Timestamp = transaction.GetAttachmentTime(),
                SecondsToMilestone = transaction.SecondsToMilestone,
                MilestoneIndexEstimate = transaction.IsMilestoneTransaction ? transaction.MilestoneIndexEstimate : -transaction.MilestoneIndexEstimate
            };

            var milestoneTransaction = new IoMilestoneTransaction<TBlobLocal>
            {                
                Partition = transaction.GetAttachmentTime(),
                ObsoleteTag = transaction.ObsoleteTag,
                Hash = transaction.Hash,
                Bundle = transaction.Bundle,
                Timestamp = transaction.GetAttachmentTime(),                
                MilestoneIndex = transaction.MilestoneIndexEstimate
            };

            if (executeBatch)
                userData = new BatchStatement();

            var batchStatement = ((BatchStatement) userData);

            if (transaction.Value != 0)
            {                
                try
                {
                    double quality;
                    try
                    {
                        quality = IoMarketDataClient.Quality + (DateTime.Now - (transaction.Timestamp.ToString().Length > 11? DateTimeOffset.FromUnixTimeMilliseconds(transaction.Timestamp): DateTimeOffset.FromUnixTimeSeconds(transaction.Timestamp))).TotalMinutes;

                        if (quality > short.MaxValue)
                            quality = short.MaxValue;
                        if (quality < short.MinValue)
                            quality = short.MinValue;
                    }
                    catch 
                    {
                        quality = short.MaxValue;
                    }

                    var draggedTransaction = new IoDraggedTransaction<TBlobLocal>
                    {
                        Address = transaction.Address,
                        Bundle = transaction.Bundle,
                        AttachmentTimestamp = transaction.AttachmentTimestamp,
                        Timestamp = transaction.Timestamp,
                        LocalTimestamp = ((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds(),
                        Value = transaction.Value,
                        Direction = (short)(transaction.CurrentIndex == transaction.LastIndex? 0:(transaction.Value>0?1:-1)),
                        Quality = (short)quality,
                        Uri = transaction.Uri,                                                
                        BtcValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Btc.Price / IoMarketDataClient.BundleSize)),
                        EthValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eth.Price / IoMarketDataClient.BundleSize)),
                        EurValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eur.Price / IoMarketDataClient.BundleSize)),
                        UsdValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Usd.Price / IoMarketDataClient.BundleSize))
                    };
                    
                    batchStatement.Add(_dragnet.Insert(draggedTransaction as IoDraggedTransaction<TBlob>));
                }
                catch (Exception e)
                {
                    _logger.Warn(e, "Unable to drag transaction:");
                }                
            }

            batchStatement.Add(_bundles.Insert((IIoTransactionModel<TBlob>) transaction));
            batchStatement.Add(_transactions.Insert(bundledHash as IoBundledHash<TBlob>));
                        
            if(transaction.BranchBuffer.Length != 0)
                batchStatement.Add(_approvees.Insert(verifiedBranchTransaction as IoApprovedTransaction<TBlob>));
            
            if (transaction.TrunkBuffer.Length != 0)
                batchStatement.Add(_approvees.Insert(verifiedTrunkTransaction as IoApprovedTransaction<TBlob>));
            
            if (transaction.AddressBuffer.Length != 0)
                batchStatement.Add(_addresses.Insert(bundledAddress as IoBundledAddress<TBlob>));
            
            if (transaction.TagBuffer.Length != 0 || transaction.IsMilestoneTransaction)
                batchStatement.Add(_tags.Insert(taggedTransaction as IoTaggedTransaction<TBlob>));

            if (transaction.IsMilestoneTransaction)
                batchStatement.Add(_milestones.Insert(milestoneTransaction as IoMilestoneTransaction<TBlob>));
                        
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
        public async Task<IIoTransactionModel<TBlobF>> GetAsync<TBlobF>(TBlobF key)        
        {
            if (!IsConfigured)
                return null;
            
            if(IoEntangled<TBlob>.Optimized)
                return (IIoTransactionModel<TBlobF>) await Mapper(async (mapper, query, args) => await mapper.FirstOrDefaultAsync<IoInteropTransactionModel>(query, args),_getTransactionQuery, key);
            else
                return (IIoTransactionModel<TBlobF>) await Mapper(async (mapper, query, args) => await mapper.FirstOrDefaultAsync<IoNativeTransactionModel>(query, args),_getTransactionQuery, key);
        }

        /// <summary>
        /// Checks whether a transaction has been loaded
        /// </summary>
        /// <typeparam name="TBlobFunc">The transaction key type</typeparam>
        /// <param name="key">The transaction key</param>
        /// <returns>True if the key was found, false otherwise</returns>
        public async Task<bool> TransactionExistsAsync<TBlobFunc>(TBlobFunc key)
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
        public async Task<IoMilestoneTransaction<TBlob>> GetMilestone(long timestamp)
        {
            if (!IsConfigured)
                return null;            
            
            return await Mapper(async (mapper, query, args) => await mapper.FirstOrDefaultAsync<IoMilestoneTransaction<TBlob>>(query, args), _getLowerMilestoneQuery, _tagsPartitioner.GetPartitionSet(timestamp), timestamp);
        }

        /// <summary>
        /// Search for the best confirming milestone worse case estimate
        /// </summary>
        /// <param name="timestamp">A nearby timestamp</param>
        /// <returns>A milestone that should confirm this timestamp</returns>
        public async Task<IoMilestoneTransaction<TBlob>> GetBestMilestoneEstimate(long timestamp)
        {
            if (!IsConfigured)
                return null;

            var lowerMilestone = await GetMilestone(timestamp);
            var queryTimestamp = lowerMilestone?.Timestamp ?? timestamp;

            var milestoneEstimates = await Mapper(async (mapper, query, args) =>
                await mapper.FetchPageAsync<IoMilestoneTransaction<TBlob>>(
                    Cql.New(query, args).WithOptions(options => options.SetPageSize(int.MaxValue))), _getHigherMilestoneQuery, _tagsPartitioner.GetPartitionSet(queryTimestamp), queryTimestamp);

            return milestoneEstimates.LastOrDefault() ?? lowerMilestone;
        }

        /// <summary>
        /// Search for the best confirming milestone worse case estimate
        /// </summary>
        /// <param name="timestamp">A nearby timestamp</param>
        /// <returns>A milestone that should confirm this timestamp</returns>
        public async Task<IIoTransactionModel<TBlob>> GetBestMilestoneEstimateBundle(long timestamp)
        {
            //Search a milestone tx
            var bestMilestone = await GetBestMilestoneEstimate(timestamp);
            if (bestMilestone == null) return null;

            //Search the bundle tx of this milestone
            var milestoneBundle = await GetAsync(bestMilestone.Bundle);
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
        public async Task RelaxZeroTransactionMilestoneEstimates(IIoTransactionModel<TBlob> milestoneTransaction)
        {
            var stopwatch = Stopwatch.StartNew();
            var milestoneLessTransactions = (await Mapper(async (mapper, query, args) => await mapper.FetchAsync<IoApprovedTransaction<TBlob>>(query, args), _getMilestoneLessTransactions, _approveePartitioner.GetPartitionSet(milestoneTransaction.Timestamp))).ToList();

            var batch = new BatchStatement();
            var batchSize = 0;
            var processedTx = 0;

            foreach (var milestoneLessTransaction in milestoneLessTransactions)
            {
                batch.Add(_relaxZeroTransactionMilestoneEstimate.Bind( -milestoneTransaction.MilestoneIndexEstimate, (long)(milestoneTransaction.GetAttachmentTime().DateTime() - milestoneLessTransaction.Timestamp.DateTime()).TotalSeconds,
                    _approveePartitioner.GetPartitionSet(milestoneTransaction.Timestamp), milestoneLessTransaction.Timestamp, milestoneLessTransaction.Hash));
                
                if (batchSize++ > 100 || ++processedTx == milestoneLessTransactions.Count) //TODO param
                {
                    var rows = await base.ExecuteAsync(batch);
                    stopwatch.Stop();
                    if(rows.Any())
                        _logger.Info($"Relaxed `{rows.GetRows().First().GetValue<long>("count")}' [zero]milestone estimates to `{milestoneTransaction.GetMilestoneIndex()}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                    batchSize = 0;                    
                }
            }
        }

        /// <summary>
        /// Relaxes zero milestone transaction estimates
        /// </summary>
        /// <param name="milestoneTransaction"></param>
        /// <returns></returns>
        public async Task RelaxTransactionMilestoneEstimates(IIoTransactionModel<TBlob> milestoneTransaction)
        {
            var stopwatch = Stopwatch.StartNew();            
            var milestoneLessTransactions = (await Mapper(async (mapper, query, args) => await mapper.FetchAsync<IoApprovedTransaction<TBlob>>(query, args), _getMilestoneTransactions, _approveePartitioner.GetPartitionSet(milestoneTransaction.GetAttachmentTime()))).ToList();

            var batch = new BatchStatement();
            var batchSize = 0;
            var processedTx = 0;

            if (milestoneLessTransactions.Count == 0)
            {
                _logger.Trace($"No transactions found to relax at `{milestoneTransaction.GetAttachmentTime().DateTime()}'");
            }

            foreach (var milestoneLessTransaction in milestoneLessTransactions)
            {
                var timeDiff = (long) (milestoneTransaction.GetAttachmentTime().DateTime() -
                                       milestoneLessTransaction.Timestamp.DateTime()).TotalSeconds;
                if (timeDiff > 0 && milestoneLessTransaction.SecondsToMilestone < timeDiff)
                {
                    processedTx++;
                    continue;                    
                }

                //batch.Add(_relaxZeroTransactionMilestoneEstimate.Bind(-milestoneTransaction.MilestoneIndexEstimate, (long)(milestoneTransaction.GetAttachmentTime().DateTime() - milestoneLessTransaction.Timestamp.DateTime()).TotalSeconds,
                //    _approveePartitioner.GetPartitionSet(milestoneTransaction.Timestamp), milestoneLessTransaction.Timestamp, milestoneLessTransaction.Hash));

                batch.Add(_relaxZeroTransactionMilestoneEstimate.Bind(-milestoneTransaction.MilestoneIndexEstimate, (long)(milestoneTransaction.GetAttachmentTime().DateTime() - milestoneLessTransaction.Timestamp.DateTime()).TotalSeconds,
                    milestoneLessTransaction.Pow, milestoneLessTransaction.Verifier,
                    _approveePartitioner.GetPartitionSet(milestoneTransaction.Timestamp), milestoneLessTransaction.Timestamp, milestoneLessTransaction.Hash));


                if (batchSize++ > 10 || ++processedTx == milestoneLessTransactions.Count) //TODO param
                {                    
                    var rows = await base.ExecuteAsync(batch);                                        
                    batchSize = 0;
                    batch = new BatchStatement();
                }
            }
            stopwatch.Stop();
            _logger.Info($"Relaxed `{processedTx}' milestones estimates to `{milestoneTransaction.GetMilestoneIndex()}', t = `{stopwatch.ElapsedMilliseconds}ms'");
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

        
        private static volatile IoTangleCassandraDb<TBlob> _default;
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>
        public static async Task<IIoDataSource<RowSet>> Default()
        {
            if (_default != null) return _default;
            
            _default = new IoTangleCassandraDb<TBlob>();
            await _default.ConnectAndConfigureAsync(IoNodeAddress.Create("tcp://10.0.75.1:9042")); //TODO config
            return _default;            
        }
    }
}
