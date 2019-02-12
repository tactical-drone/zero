using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using NLog;
using zero.core.data.contracts;
using zero.core.data.market;
using zero.core.data.providers.cassandra;
using zero.core.models;
using zero.core.network.ip;
using zero.interop.entangled;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using zero.tangle.data.cassandra.tangle.luts;
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
        private Table<IIoTransactionModel<TBlob>> _transactions;
        private Table<IoBundledHash<TBlob>> _hashes;
        private Table<IoBundledAddress<TBlob>> _addresses;
        private Table<IoTaggedTransaction<TBlob>> _tags;
        private Table<IoApprovedTransaction<TBlob>> _verifiers;
        private Table<IoDraggedTransaction<TBlob>> _dragnet;

        private PreparedStatement _dupCheckQuery;
        private readonly string _getTransactionQuery = $"select * from bundle where bundle=? order by currentindex limit 1 ALLOW FILTERING";
        private readonly string _getClosestMilestoneQuery = $"select * from tag where partition IN ? and ismilestonetransaction = true LIMIT 1 ALLOW FILTERING";

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
                
                _transactions = new Table<IIoTransactionModel<TBlob>>(_session, _ioTangleKeySpace.BundleMap);
                if (!existingTables.Contains(_transactions.Name))
                {
                    _transactions.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_transactions.Name}'");
                    wasConfigured = false;
                }
                
                                                                    
                _hashes = new Table<IoBundledHash<TBlob>>(_session, _ioTangleKeySpace.BundledTransaction);
                if (!existingTables.Contains(_hashes.Name))
                {
                    _hashes.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_hashes.Name}'");
                    wasConfigured = false;
                }
                    
                _addresses = new Table<IoBundledAddress<TBlob>>(_session, _ioTangleKeySpace.BundledAddressMap);
                if (!existingTables.Contains(_addresses.Name))
                {
                    _addresses.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_addresses.Name}'");
                    wasConfigured = false;
                }
                
                _tags = new Table<IoTaggedTransaction<TBlob>>(_session, _ioTangleKeySpace.TaggedTransaction);
                if (!existingTables.Contains(_tags.Name))
                {
                    _tags.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_tags.Name}'");
                    wasConfigured = false;
                }

                _verifiers = new Table<IoApprovedTransaction<TBlob>>(_session, _ioTangleKeySpace.ApprovedTransaction);
                if (!existingTables.Contains(_verifiers.Name))
                {
                    _verifiers.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_verifiers.Name}'");
                    wasConfigured = false;
                }

                _dragnet = new Table<IoDraggedTransaction<TBlob>>(_session, _ioTangleKeySpace.DraggedTransactionMap);
                if (!existingTables.Contains(_dragnet.Name))
                {
                    _dragnet.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_dragnet.Name}'");
                    wasConfigured = false;
                }
                
                _dupCheckQuery = _session.Prepare($"select count(*) from {_hashes.Name} WHERE {nameof(IoBundledHash<TBlob>.Hash)}=? LIMIT 1");
            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to ensure schema:");
                return true;                
            }

            _logger.Trace("Ensured schema!");

            IsConfigured = true;

            return wasConfigured;
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
                Timestamp = transaction.Timestamp,
                MilestoneIndex = transaction.IsMilestoneTransaction ? transaction.MilestoneIndexEstimate : -transaction.MilestoneIndexEstimate
            };

            var bundledAddress = new IoBundledAddress<TBlobLocal>
            {
                Address = transaction.Address,
                Bundle = transaction.Bundle,
                Timestamp = transaction.Timestamp
            };
            
            var taggedTransaction = new IoTaggedTransaction<TBlobLocal>
            {
                Tag = transaction.Tag,
                Partition = (long)Math.Truncate(transaction.Timestamp/3600.0) * 3600,
                ObsoleteTag = transaction.ObsoleteTag,
                Hash = transaction.Hash,
                Bundle = transaction.Bundle,
                Timestamp = transaction.Timestamp,
                IsMilestoneTransaction = transaction.IsMilestoneTransaction,
                MilestoneIndex = transaction.IsMilestoneTransaction ? transaction.MilestoneIndexEstimate : - transaction.MilestoneIndexEstimate //negative indicates that this is an initial estimate
            };

            var partition = (long)Math.Truncate( transaction.AttachmentTimestamp > 0 ? transaction.AttachmentTimestamp: transaction.Timestamp / 600.0) * 600;             
            var verifiedBranchTransaction = new IoApprovedTransaction<TBlobLocal>
            {
                Partition = partition,
                Hash = transaction.Branch,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,
                MilestoneIndexEstimate = transaction.IsMilestoneTransaction ? transaction.MilestoneIndexEstimate : -transaction.MilestoneIndexEstimate
            };

            var verifiedTrunkTransaction = new IoApprovedTransaction<TBlobLocal>
            {
                Partition = partition,
                Hash = transaction.Trunk,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,                
                MilestoneIndexEstimate = transaction.IsMilestoneTransaction ? transaction.MilestoneIndexEstimate : -transaction.MilestoneIndexEstimate
            };

            if (executeBatch)
                userData = new BatchStatement();

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
                        LocalTimestamp = ((DateTimeOffset)DateTime.Now).ToUnixTimeSeconds(),
                        Value = transaction.Value,
                        Direction = (short)(transaction.CurrentIndex == transaction.LastIndex? 0:(transaction.Value>0?1:-1)),
                        Quality = (short)quality,
                        Uri = transaction.Uri,                                                
                        BtcValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Btc.Price / IoMarketDataClient.BundleSize)),
                        EthValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eth.Price / IoMarketDataClient.BundleSize)),
                        EurValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eur.Price / IoMarketDataClient.BundleSize)),
                        UsdValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Usd.Price / IoMarketDataClient.BundleSize))
                    };
                    
                    ((BatchStatement)userData).Add(_dragnet.Insert(draggedTransaction as IoDraggedTransaction<TBlob>));
                }
                catch (Exception e)
                {
                    _logger.Warn(e, "Unable to drag transaction:");
                }                
            }

            ((BatchStatement)userData).Add(_transactions.Insert((IIoTransactionModel<TBlob>) transaction));
            ((BatchStatement)userData).Add(_hashes.Insert(bundledHash as IoBundledHash<TBlob>));
                        
            if(transaction.BranchBuffer.Length != 0)
                ((BatchStatement)userData).Add(_verifiers.Insert(verifiedBranchTransaction as IoApprovedTransaction<TBlob>));
            
            if (transaction.TrunkBuffer.Length != 0)
                ((BatchStatement)userData).Add(_verifiers.Insert(verifiedTrunkTransaction as IoApprovedTransaction<TBlob>));
            
            if (transaction.AddressBuffer.Length != 0)
                ((BatchStatement)userData).Add(_addresses.Insert(bundledAddress as IoBundledAddress<TBlob>));
            
            if (transaction.TagBuffer.Length != 0 || transaction.IsMilestoneTransaction)
                ((BatchStatement)userData).Add(_tags.Insert(taggedTransaction as IoTaggedTransaction<TBlob>));
                        
            if (executeBatch)
            {                
                return await ExecuteAsync((BatchStatement)userData);                
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
                return (IIoTransactionModel<TBlobF>) await Mapper(async mapper => await mapper.FirstOrDefaultAsync<IoInteropTransactionModel>(_getTransactionQuery, key));
            else
                return (IIoTransactionModel<TBlobF>) await Mapper(async mapper => await mapper.FirstOrDefaultAsync<IoNativeTransactionModel>(_getTransactionQuery, key));
        }

        //public async Task<IIoTransactionModel<TBlob>> GetAsync2(TBlob key)
        //{
        //    if (!IsConfigured)
        //        return null;

        //    if (IoEntangled<TBlob>.Optimized)
        //        return (IIoTransactionModel<TBlob>)await Mapper(mapper => mapper.FirstOrDefaultAsync<IoInteropTransactionModel>(_getTransactionQuery, key));
        //    else
        //        return (IIoTransactionModel<TBlob>)await Mapper(mapper => mapper.FirstOrDefaultAsync<IoNativeTransactionModel>(_getTransactionQuery, key));
        //}

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
        /// Search for the closest milestone
        /// </summary>
        /// <param name="timestamp">The nearby timestamp</param>
        /// <returns>The nearest milestone transaction</returns>
        public async Task<IoTaggedTransaction<TBlob>> GetClosestMilestone(long timestamp)
        {
            if (!IsConfigured)
                return null;

            var partitionSize = 3600;
            var partition = (long)Math.Truncate(timestamp / (double)partitionSize) * partitionSize;

            try
            {
                return await Mapper(async (mapper) => await mapper.FirstOrDefaultAsync<IoTaggedTransaction<TBlob>>(_getClosestMilestoneQuery, new[] {partition - partitionSize, partition, partition + partitionSize}));
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Get closest milestone query failed: ");
                IsConnected = false;
                return null;
            }
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
