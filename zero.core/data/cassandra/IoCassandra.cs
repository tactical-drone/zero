using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using NLog;
using zero.core.data.contracts;
using zero.core.data.lookups;
using zero.core.network.ip;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using Logger = NLog.Logger;

namespace zero.core.data.cassandra
{
    public class IoCassandra: IoCassandraBase, IIoData
    {
        /// <summary>
        /// 
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
                MappingConfiguration.Global.Define(new Map<IoMarshalledTransaction>().TableName("bundle")
                    .PartitionKey(b=>b.bundle,b=>b.current_index));

                var existingTables = _cluster.Metadata.GetKeyspace(Keyspace).GetTablesNames();

                _transactions = new Table<IoMarshalledTransaction>(_session);
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

        public async Task<RowSet> Put(IoInteropTransactionModel interopTransaction)
        {
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

            var batch = new BatchStatement();
            
            batch.Add(_transactions.Insert(interopTransaction.Mapping));
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
                        _logger.Error(r.Exception, "Put data returned with errors:");
                        break;
                    case TaskStatus.RanToCompletion:
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            });

            return await retval;
        }

        public Task<IoInteropTransactionModel> Get(string key)
        {
            throw new NotImplementedException();
        }
        
        private static IoCassandra _default;
        public static async Task<IIoData> Default()
        {
            if (_default != null) return _default;

            _default = new IoCassandra();
            await _default.Connect("tcp://10.0.75.1:9042");
            return _default;
        }
    }
}
