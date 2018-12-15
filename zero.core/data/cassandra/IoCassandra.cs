using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using NLog;
using zero.core.data.contracts;
using zero.core.data.lookups;
using zero.core.data.market;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.mock;
using Logger = NLog.Logger;

namespace zero.core.data.cassandra
{
    public class IoCassandra: IoCassandraBase, IIoData
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoCassandra()
        {
            _logger = LogManager.GetCurrentClassLogger();
            Keyspace = "zero";
        }

        private readonly Logger _logger;
        
        private Table<IoMarshalledTransaction> _transactions;
        private Table<IoHashedBundle> _hashes;
        private Table<IoBundledAddress> _addresses;
        private Table<IoTaggedTransaction> _tags;
        private Table<IoVerifiedTransaction> _verifiers;
        private Table<IoDraggedTransaction> _dragnet;

        protected override bool EnsureSchema()
        {   
            _logger.Debug("Ensuring db schema...");
            bool wasConfigured = true;

            //ensure keyspace
            var replicationConfig = new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"}, {"replication_factor", "1"}
            };

            _session.CreateKeyspaceIfNotExists(Keyspace, replicationConfig, false);
            _session.ChangeKeyspace(Keyspace);
            
            //ensure tables            
            try
            {                                
                var config = new Map<IoMarshalledTransaction>().TableName("bundle")                    
                    .ExplicitColumns()                    
                    .Column(c => c.Body, map => map.WithName(nameof(IoMarshalledTransaction.signature_or_message)))
                    .Column(c => c.address)
                    .Column(c => c.value)
                    .Column(c => c.timestamp)
                    .Column(c => c.current_index)
                    .Column(c => c.last_index)
                    .Column(c => c.bundle)
                    .Column(c => c.trunk)
                    .Column(c => c.branch)
                    .Column(c => c.Tag, map => map.AsFrozen())
                    .Column(c => c.attachment_timestamp)
                    .Column(c => c.attachment_timestamp_lower)
                    .Column(c => c.attachment_timestamp_upper)
                    .Column(c => c.nonce)
                    .Column(c => c.hash)
                    .Column(c=>c.Size)

                    .PartitionKey(b => b.bundle, b=>b.address)

                    .ClusteringKey(c => c.last_index, SortOrder.Descending)
                    .ClusteringKey(c =>c.current_index, SortOrder.Ascending)                    
                    .ClusteringKey(c=>c.value,SortOrder.Descending)
                    .ClusteringKey(c=>c.Tag,SortOrder.Ascending)
                    .ClusteringKey(c=>c.hash,SortOrder.Ascending)                    
                    .ClusteringKey(c=>c.attachment_timestamp, SortOrder.Descending)
                    .ClusteringKey(c=>c.attachment_timestamp_lower, SortOrder.Ascending)
                    .ClusteringKey(c=>c.attachment_timestamp_upper, SortOrder.Descending)
                    .ClusteringKey(c=>c.timestamp, SortOrder.Descending);

                var mapConfig = new MappingConfiguration();
                mapConfig.Define(config);

                var existingTables = _cluster.Metadata.GetKeyspace(Keyspace).GetTablesNames();

                _transactions = new Table<IoMarshalledTransaction>(_session, mapConfig);
                if (!existingTables.Contains("bundle"))
                {
                    _transactions.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_transactions.Name}'");
                    wasConfigured = false;
                }
                                                                    
                _hashes = new Table<IoHashedBundle>(_session);
                if (!existingTables.Contains("transaction"))
                {
                    _hashes.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_hashes.Name}'");
                    wasConfigured = false;
                }
                    
                _addresses = new Table<IoBundledAddress>(_session);
                if (!existingTables.Contains("address"))
                {
                    _addresses.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_addresses.Name}'");
                    wasConfigured = false;
                }

                _tags = new Table<IoTaggedTransaction>(_session);
                if (!existingTables.Contains("tag"))
                {
                    _tags.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_tags.Name}'");
                    wasConfigured = false;
                }

                _verifiers = new Table<IoVerifiedTransaction>(_session);
                if (!existingTables.Contains("verifier"))
                {
                    _verifiers.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_verifiers.Name}'");
                    wasConfigured = false;
                }

                _dragnet = new Table<IoDraggedTransaction>(_session);
                if (!existingTables.Contains("dragnet"))
                {
                    _dragnet.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_dragnet.Name}'");
                    wasConfigured = false;
                }

            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to ensure schema:");
                return true;                
            }

            _logger.Trace("Ensured schema!");
            return wasConfigured;
        }

        public new bool IsConnected => base.IsConnected;

        public async Task<RowSet> Put(IoInteropTransactionModel interopTransaction, object batch = null)
        {
            var executeBatch = batch == null;

            var hashedBundle = new IoHashedBundle
            {
                Hash = interopTransaction.Mapping.hash,
                Bundle = interopTransaction.Mapping.bundle               
            };

            var bundledAddress = new IoBundledAddress
            {
                Address = interopTransaction.Mapping.address,
                Bundle = interopTransaction.Mapping.bundle                
            };

            var taggedTransaction = new IoTaggedTransaction
            {
                Tag = interopTransaction.Mapping.Tag,
                Hash = interopTransaction.Mapping.hash
            };

            var verifiedBranchTransaction = new IoVerifiedTransaction
            {
                Hash = interopTransaction.Mapping.branch,
                Pow = interopTransaction.Pow,
                Verifier = interopTransaction.Mapping.hash
            };

            var verifiedTrunkTransaction = new IoVerifiedTransaction
            {
                Hash = interopTransaction.Mapping.trunk,
                Pow = interopTransaction.Pow,
                Verifier = interopTransaction.Mapping.hash
            };

            if (executeBatch)
                batch = new BatchStatement();

            if (interopTransaction.Value != 0)
            {                
                try
                {
                    var draggedTransaction = new IoDraggedTransaction
                    {
                        Hash = interopTransaction.Mapping.hash,
                        Uri = interopTransaction.Uri,
                        Size =  interopTransaction.Mapping.Size,
                        Value = interopTransaction.Value,
                        attachment_timestamp = interopTransaction.Mapping.attachment_timestamp,
                        Tag = interopTransaction.Mapping.Tag,
                        timestamp = interopTransaction.Mapping.timestamp,
                        attachment_timestamp_lower = interopTransaction.Mapping.attachment_timestamp_lower,
                        attachment_timestamp_upper = interopTransaction.Mapping.attachment_timestamp_upper,
                        Quality = IoMarketDataClient.Quality,
                        BtcValue = (float)(interopTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Btc.Price / IoMarketDataClient.BundleSize)),
                        EthValue = (float)(interopTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eth.Price / IoMarketDataClient.BundleSize)),
                        EurValue = (float)(interopTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eur.Price / IoMarketDataClient.BundleSize)),
                        UsdValue = (float)(interopTransaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Usd.Price / IoMarketDataClient.BundleSize))
                    };
                    
                    ((BatchStatement)batch).Add(_dragnet.Insert(draggedTransaction));
                }
                catch (Exception e)
                {
                    _logger.Warn(e, "Unable to drag transaction:");
                }                
            }            
                        
            ((BatchStatement)batch).Add(_transactions.Insert(interopTransaction.Mapping));
            ((BatchStatement)batch).Add(_hashes.Insert(hashedBundle));
            ((BatchStatement)batch).Add(_addresses.Insert(bundledAddress));
            ((BatchStatement)batch).Add(_tags.Insert(taggedTransaction));
            ((BatchStatement)batch).Add(_verifiers.Insert(verifiedBranchTransaction));
            ((BatchStatement)batch).Add(_verifiers.Insert(verifiedTrunkTransaction));
            
            if (executeBatch)
            {
                await ExecuteAsync((BatchStatement)batch);
            }

            return null;
        }

        public Task<IoInteropTransactionModel> Get(string key)
        {
            throw new NotImplementedException();
        }

        public async Task<RowSet> ExecuteAsync(object batch)
        {
            return await base.ExecuteAsync((BatchStatement)batch);
        }

        private static volatile IoCassandra _default;
        public static async Task<IIoData> Default()
        {
            if (_default != null) return _default;

            _default = new IoCassandra();
            await _default.Connect("tcp://10.0.75.1:9042");
            return _default;
        }
    }
}
