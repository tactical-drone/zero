using System;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.network.ip;
using Logger = NLog.Logger;

namespace zero.core.data.cassandra
{
    /// <summary>
    /// Based to support different transaction models
    /// </summary>
    /// <typeparam name="TBlob">Blob type</typeparam>
    public abstract class IoCassandraBase<TBlob> : IoCassandraKeyBase<TBlob>
    {
        protected IoCassandraBase()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;

        protected string Keyspace;
        protected volatile Cluster _cluster;
        protected ISession _session;
        protected IoNodeAddress _clusterAddress;
        
        protected volatile bool _isConnecting = false;
        private volatile bool _isConnected = false;
        private DateTime _lastConnectionAttempt = DateTime.Now - TimeSpan.FromSeconds(30);

        /// <summary>
        /// Returns true if everything is working and connected
        /// </summary>
        public bool IsConnected
        {
            get => _isConnected;
            protected set
            {
                //Quick retry
                if (value == false && _isConnected)
                    _cluster = null;                

                _isConnected = value;
            }
        }        

        /// <summary>
        /// Connects to a cassandra database
        /// </summary>
        /// <param name="url">The url of the database</param>
        /// <returns>True if succeeded, false otherwise</returns>
        protected async Task<bool> Connect(IoNodeAddress url)
        {
            if (_isConnecting || !_isConnected && (DateTime.Now - _lastConnectionAttempt) < TimeSpan.FromSeconds(10))
                return false;
            _lastConnectionAttempt = DateTime.Now;            
            _isConnecting = true;
            
            _clusterAddress = IoNodeAddress.Create(url.Url);
            _cluster = Cluster.Builder().AddContactPoint(_clusterAddress.IpEndPoint).Build();            

            _logger.Debug("Connecting to Cassandra...");

            try
            {
                _session = await _cluster.ConnectAsync();
            }
            catch (Exception e)
            {
                _cluster = null;                
                _logger.Error(e, $"Unable to connect to cassandra database `{_clusterAddress.Url}' at `{_clusterAddress.ResolvedIpAndPort}':");
                
                return _isConnecting = false;
            }
            
            _logger.Debug($"Connected to Cassandra cluster = `{_cluster.Metadata.ClusterName}'");

            if (!await EnsureSchema())
                _logger.Info("Configured db schema");

            IsConnected = true;
            _isConnecting = false;

            return true;
        }

        /// <summary>
        /// Makes sure that the schema is configured
        /// </summary>
        /// <returns>True if the schema was re-configured, false otherwise</returns>
        protected abstract Task<bool> EnsureSchema();

        /// <summary>
        /// Puts data into cassandra
        /// </summary>
        /// <param name="transaction">The transaction to persist</param>
        /// <param name="batch">A batch handler</param>
        /// <returns>The rowset with insert results</returns>
        public async Task<RowSet> ExecuteAsync(BatchStatement batch)
        {
            var executeAsyncTask = _session.ExecuteAsync(batch);
#pragma warning disable 4014
            executeAsyncTask.ContinueWith(r =>
#pragma warning restore 4014
            {
                switch (r.Status)
                {
                    case TaskStatus.Canceled:
                    case TaskStatus.Faulted:
                        _logger.Error(r.Exception, "Put data returned with errors:");
                        break;
                }
            });
            return await executeAsyncTask;
        }
    }
}
