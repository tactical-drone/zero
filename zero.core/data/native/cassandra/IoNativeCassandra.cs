using System;
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

            _logger.Trace("Ensured schema!");
            return wasConfigured;
        }

        public new bool IsConnected => base.IsConnected;

        public async Task<RowSet> Put(IoNativeTransactionModel transaction)
        {
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

            var batch = new BatchStatement();
            
            batch.Add(_transactions.Insert(transaction));
            batch.Add(_hashes.Insert(hashedBundle));
            batch.Add(_addresses.Insert(bundledAddress));

            var retval = _session.ExecuteAsync(batch);
#pragma warning disable 4014
            retval.ContinueWith(r =>
#pragma warning restore 4014
            {
                switch (r.Status)
                {
                    case TaskStatus.Canceled:
                    case TaskStatus.Faulted:
                        _logger.Error(r.Exception,"Put data returned with errors:");
                        break;
                    case TaskStatus.RanToCompletion:
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            });

            return await retval;
        }

        public Task<IoNativeTransactionModel> Get(string key)
        {
            throw new NotImplementedException();
        }

        private static IoNativeCassandra _default;
        public static async Task<IIoNativeData> Default()
        {
            if (_default != null) return _default;

            _default = new IoNativeCassandra();
            await _default.Connect("tcp://11.0.75.1:9042");
            return _default;
        }
    }
}
