using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using NLog;
using zero.core.data.native.contracts;
using zero.core.data.native.lookups;
using zero.core.network.ip;
using zero.interop.entangled.common.model.native;
using Logger = NLog.Logger;

namespace zero.core.data.native.cassandra
{
    public class IoNativeCassandra:IIoNativeData
    {
        /// <summary>
        /// 
        /// </summary>
        public IoNativeCassandra()
        {
            _logger = LogManager.GetCurrentClassLogger();            
        }

        private readonly Logger _logger;
        private Cluster _cluster;
        private ISession _session;
        private IMapper _mapper;
        private IoNodeAddress _clusterAddress;

        private Table<IoNativeTransactionModel> _transactions;
        private Table<IoNativeHashedBundle> _hashes;
        private Table<IoNativeBundledAddress> _addresses;

        private async Task<bool> Connect(string url)
        {
            if (_cluster != null)
            {
                _logger.Error($"Cluster already connected at {_clusterAddress.IpEndPoint}");
                return false;
            }
            
            _clusterAddress = IoNodeAddress.Create(url);
            try
            {
                _logger.Trace("Building cassandra connection...");
                _cluster = Cluster.Builder().AddContactPoint(_clusterAddress.IpEndPoint).Build();
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to connect to cassandra cluster at `{_clusterAddress.IpEndPoint}':");
                return false;
            }

            _logger.Trace("Connecting to Cassandra...");
            _session = await _cluster.ConnectAsync();
            _logger.Info("Connected to Cassandra!");

            _mapper = new Mapper(_session);

            EnsureSchema();

            return true;
        }

        private void EnsureSchema()
        {   
            _logger.Trace("Ensuring schema...");

            //ensure keyspace
            var replicationConfig = new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"}, {"replication_factor", "1"}
            };

            _session.CreateKeyspaceIfNotExists("one", replicationConfig, false);
            _session.ChangeKeyspace("one");

            //ensure tables
            _transactions = new Table<IoNativeTransactionModel>(_session);
            _transactions.CreateIfNotExists();
            _hashes = new Table<IoNativeHashedBundle>(_session);
            _hashes.CreateIfNotExists();
            _addresses = new Table<IoNativeBundledAddress>(_session);
            _addresses.CreateIfNotExists();

            _logger.Trace("Ensured schema!");
        }

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
            retval.ContinueWith(r =>
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
            await _default.Connect("tcp://10.0.75.1:9042");
            return _default;
        }
    }
}
