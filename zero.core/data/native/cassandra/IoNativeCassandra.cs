﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using NLog;
using zero.core.data.cassandra;
using zero.core.data.native.contracts;
using zero.core.data.native.lookups;
using zero.core.network.ip;
using zero.interop.entangled.common.model.native;
using Logger = NLog.Logger;

namespace zero.core.data.native.cassandra
{
    public class IoNativeCassandra: IoCassandraBase, IIoNativeData
    {
        /// <summary>
        /// 
        /// </summary>
        public IoNativeCassandra()
        {
            _logger = LogManager.GetCurrentClassLogger();
            Keyspace = "one";
        }

        private readonly Logger _logger;        
        
        private Table<IoNativeTransactionModel> _transactions;
        private Table<IoNativeHashedBundle> _hashes;
        private Table<IoNativeBundledAddress> _addresses;
        private Table<IoNativeTaggedTransaction> _tags;
       
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

            var existingTables = _cluster.Metadata.GetKeyspace(Keyspace).GetTablesNames();

            //ensure tables
            _transactions = new Table<IoNativeTransactionModel>(_session);
            if (!existingTables.Contains("bundle"))
            {
                _transactions.CreateIfNotExists();
                _logger.Debug($"Adding table `{_transactions.Name}'");
                wasConfigured = false;
            }
                
            _hashes = new Table<IoNativeHashedBundle>(_session);
            if (!existingTables.Contains("transaction"))
            {
                _hashes.CreateIfNotExists();
                _logger.Debug($"Adding table `{_hashes.Name}'");
                wasConfigured = false;
            }
            
            _addresses = new Table<IoNativeBundledAddress>(_session);
            if (!existingTables.Contains("address"))
            {
                _addresses.CreateIfNotExists();
                _logger.Debug($"Adding table `{_addresses.Name}'");
                wasConfigured = false;
            }

            _tags = new Table<IoNativeTaggedTransaction>(_session);
            if (!existingTables.Contains("tag"))
            {
                _tags.CreateIfNotExists();
                _logger.Debug($"Adding table `{_tags.Name}'");
                wasConfigured = false;
            }

            if(!wasConfigured)
                _logger.Trace("Ensured schema!");

            return wasConfigured;
        }

        public new bool IsConnected => base.IsConnected;

        public async Task<RowSet> Put(IoNativeTransactionModel transaction, object batch = null)
        {
            bool executeBatch = batch == null;

            var hashedBundle = new IoNativeHashedBundle
            {
                Hash = transaction.Hash,
                Bundle = transaction.Bundle
            };

            var bundledAddress = new IoNativeBundledAddress
            {
                Address = transaction.Address,
                Bundle = transaction.Bundle
            };

            var taggedTransaction = new IoNativeTaggedTransaction
            {
                Tag = transaction.Tag,
                Hash = transaction.Hash
            };

            if(batch == null)
                batch = new BatchStatement();
            
            ((BatchStatement)batch).Add(_transactions.Insert(transaction));
            ((BatchStatement)batch).Add(_hashes.Insert(hashedBundle));
            ((BatchStatement)batch).Add(_addresses.Insert(bundledAddress));
            ((BatchStatement)batch).Add(_tags.Insert(taggedTransaction));

            if (executeBatch)
            {
                await ExecuteAsync(batch);
            }

            return null;
        }

        public Task<IoNativeTransactionModel> Get(string key)
        {
            throw new NotImplementedException();
        }

        private static volatile IoNativeCassandra _default;
        public static async Task<IIoNativeData> Default()
        {
            if (_default != null) return _default;

            _default = new IoNativeCassandra();
            await _default.Connect("tcp://10.0.75.1:9042");
            return _default;
        }

        public new async Task<RowSet> ExecuteAsync(object batch)
        {
            return await base.ExecuteAsync((BatchStatement)batch);
        }
    }
}
