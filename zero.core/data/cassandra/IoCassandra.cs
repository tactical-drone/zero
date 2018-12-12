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
    public class IoCassandra:IIoData
    {
        /// <summary>
        /// 
        /// </summary>
        public IoCassandra()
        {
            _logger = LogManager.GetCurrentClassLogger();            
        }

        private readonly Logger _logger;
        private Cluster _cluster;
        private ISession _session;
        private IMapper _mapper;
        private IoNodeAddress _clusterAddress;

        private Table<IoTransactionModel> _transactions;
        private Table<IoHashedBundle> _hashes;
        private Table<IoBundledAddress> _addresses;

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

            _session.CreateKeyspaceIfNotExists("zero", replicationConfig, false);
            _session.ChangeKeyspace("zero");

            //ensure tables

            try
            {
                _transactions = new Table<IoTransactionModel>(_session);            
                _transactions.CreateIfNotExists();
                _hashes = new Table<IoHashedBundle>(_session);
                _hashes.CreateIfNotExists();
                _addresses = new Table<IoBundledAddress>(_session);
                _addresses.CreateIfNotExists();
            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to ensure schema:");
                return;                
            }

            _logger.Trace("Ensured schema!");
        }

        public async Task<RowSet> Put(IoTransactionModel transaction)
        {
            var hashedBundle = new IoHashedBundle
            {
                Hash = transaction.hash,
                Bundle = transaction.bundle               
            };

            var bundledAddress = new IoBundledAddress
            {
                Address = transaction.address,
                Bundle = transaction.bundle                
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

        public Task<IoTransactionModel> Get(string key)
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
