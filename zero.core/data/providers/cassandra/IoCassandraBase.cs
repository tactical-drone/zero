using System;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.conf;
using zero.core.data.providers.cassandra.keyspaces.tangle;
using zero.core.network.ip;
using Logger = NLog.Logger;

namespace zero.core.data.providers.cassandra
{
    /// <summary>
    /// Based to support different transaction models
    /// </summary>
    /// <typeparam name="TBlob">Blob type</typeparam>
    public abstract class IoCassandraBase<TBlob> : IoConfigurable
    {
        protected IoCassandraBase(IoTangleKeySpace<TBlob> keySpaceConfiguration)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _keySpaceConfiguration = keySpaceConfiguration;            
        }

        private readonly Logger _logger;
                
        protected IoTangleKeySpace<TBlob> _keySpaceConfiguration;
        protected volatile Cluster _cluster;
        protected ISession _session;
        protected IoNodeAddress _clusterAddress;
        
        protected volatile bool _isConnecting = false;
        private volatile bool _isConnected = false;
        private DateTime _lastConnectionAttempt = DateTime.Now - TimeSpan.FromSeconds(30);

        /// <summary>
        /// Used to passively ignore missing redis configurations
        /// </summary>
        private int _connectionAttempts = 0;

        /// <summary>
        /// Maximum amount of connection retries before giving up
        /// </summary>
        private const int MaxConnectionAttempts = 3;

        [IoParameter]
        public int parm_cassandra_connection_retry_wait_time_ms = 30000;

        /// <summary>
        /// Returns true if everything is working and connected
        /// </summary>
        public bool IsConnected
        {
            get => _isConnected;
            protected set
            {                
                if (value == false && _isConnected)
                {
                    try
                    {
                        _cluster?.Dispose();
                        _cluster = null;

                        if(_session?.IsDisposed??false)
                            _session.Dispose();
                        _session = null;
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e,$"An error occurred while disposing cassandra connection `{_clusterAddress}':");
                    }
                }                    

                _isConnected = value;
            }
        }  
        
        /// <summary>
        /// True if the schema has been ensured, false otherwise
        /// </summary>
        public bool IsConfigured { get; protected set; }

        /// <summary>
        /// Connects to a cassandra database
        /// </summary>
        /// <param name="clusterAddress">The url of the database</param>
        /// <returns>True if succeeded, false otherwise</returns>
        protected async Task<bool> ConnectAndConfigureAsync(IoNodeAddress clusterAddress)
        {
            //Used for now to ignore missing cassandra hosts
            if (_connectionAttempts >= MaxConnectionAttempts)
                return false;

            if (_isConnecting || !_isConnected && (DateTime.Now - _lastConnectionAttempt) < TimeSpan.FromMilliseconds(parm_cassandra_connection_retry_wait_time_ms))
                return false;

            IsConfigured = false;
            IsConnected = false;

            _clusterAddress = clusterAddress;
            _lastConnectionAttempt = DateTime.Now;            
            _isConnecting = true;
            
                        
            _cluster = Cluster.Builder().AddContactPoint(_clusterAddress.IpEndPoint).Build();            

            _logger.Debug("Connecting to Cassandra...");

            try
            {
                _connectionAttempts++;
                _session = await _cluster.ConnectAsync();
            }
            catch (Exception e)
            {                
                _logger.Error(e, $"Unable to connect to cassandra database `{_clusterAddress.Url}' at `{_clusterAddress.ResolvedIpAndPort}':");
                IsConnected = false;
                return _isConnecting = false;
            }

            _connectionAttempts = 0;
            IsConnected = true;

            _logger.Info($"Connected to Cassandra cluster `{_cluster.Metadata.ClusterName}'");

            if (!await EnsureSchemaAsync())
                _logger.Info($"Configured keyspace `{_keySpaceConfiguration.Name}'");
            
            _isConnecting = false;

            return true;
        }

        public async Task<bool> EnsureDatabaseAsync()
        {
            if (IsConnected && IsConfigured)
                return true;

            return await ConnectAndConfigureAsync(_clusterAddress);
        }

        /// <summary>
        /// Makes sure that the schema is configured
        /// </summary>
        /// <returns>True if the schema was re-configured, false otherwise</returns>
        protected abstract Task<bool> EnsureSchemaAsync();

        /// <summary>
        /// Executes a cassandra batch statement
        /// </summary>        
        /// <param name="batch">A batch handler</param>
        /// <returns>The <see cref="RowSet"/> containing transaction results</returns>
        public async Task<RowSet> ExecuteAsync(BatchStatement batch)
        {
            if (!await EnsureDatabaseAsync())
                return null;

            Task<RowSet> executeAsyncTask;

            try
            {
                executeAsyncTask = _session.ExecuteAsync(batch);
#pragma warning disable 4014
                executeAsyncTask.ContinueWith(r =>
#pragma warning restore 4014
                {
                    switch (r.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(r.Exception, "Batch statement returned with errors:");
                            IsConnected = false;
                            break;
                    }
                });
            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to execute batch statement:");
                IsConnected = false; //TODO, does cassandra have auto retry connect?
                return null;
            }            

            return await executeAsyncTask;
        }
    }
}
