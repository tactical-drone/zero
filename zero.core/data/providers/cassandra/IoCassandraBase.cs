using System;
using System.Collections;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Mapping;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.network.ip;
using Logger = NLog.Logger;
using System.Linq;
using System.Text;
using zero.core.patterns.misc;

namespace zero.core.data.providers.cassandra
{
    /// <summary>
    /// Based to support different transaction models
    /// </summary>
    /// <typeparam name="TKey">key type</typeparam>
    public abstract class IoCassandraBase<TKey> : IoNanoprobe
    {
        protected IoCassandraBase(IIoCassandraKeySpace keySpaceConfiguration) : base($"{nameof(IoCassandraBase<TKey>)}",1)
        {
            _logger = LogManager.GetCurrentClassLogger();
            _keySpaceConfiguration = keySpaceConfiguration;            
        }

        private readonly Logger _logger;
                
        protected IIoCassandraKeySpace _keySpaceConfiguration;
        protected volatile Cluster _cluster;
        protected ISession _session;
        private   IMapper _mapper;
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
                        _mapper = null;

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
            
            IsConnected = false;

            _clusterAddress = clusterAddress;
            _lastConnectionAttempt = DateTime.Now;            
            _isConnecting = true;
            
                        
            _cluster = Cluster.Builder().AddContactPoint(_clusterAddress.IpEndPoint).Build();            

            _logger.Trace("Connecting to Cassandra...");

            try
            {
                _connectionAttempts++;
                _session = await _cluster.ConnectAsync().ConfigureAwait(CfgAwait);                
                _mapper = new Mapper(_session);                
            }
            catch (Exception e)
            {                
                _logger.Error(e, $"Unable to connect to cassandra database `{_clusterAddress.Url}' at `{_clusterAddress.EndpointIpPort}':");
                IsConnected = false;
                return _isConnecting = false;
            }

            _connectionAttempts = 0;
            IsConnected = true;

            _logger.Info($"Connected to Cassandra cluster `{_cluster.Metadata.ClusterName}'");

            if (!await EnsureSchemaAsync().ConfigureAwait(CfgAwait))
                _logger.Info($"Configured keyspace `{_keySpaceConfiguration.Name}'");
            
            _isConnecting = false;

            return true;
        }

        public async Task<bool> EnsureDatabaseAsync()
        {            
            if (IsConnected && IsConfigured)
                return true;

            return await ConnectAndConfigureAsync(_clusterAddress).ConfigureAwait(CfgAwait);
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
        public async Task<RowSet> ExecuteAsync(IStatement batch)
        {
            if (!await EnsureDatabaseAsync().ConfigureAwait(CfgAwait))
                return null;

            try
            {
                var executeAsyncTask = _session.ExecuteAsync(batch);

                await executeAsyncTask.ContinueWith(r =>

                {
                    switch (r.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(r.Exception, "Batch statement returned with errors:");
                            IsConnected = false;
                            break;
                    }
                }).ConfigureAwait(CfgAwait);

                return await executeAsyncTask.ConfigureAwait(CfgAwait);
            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to execute batch statement:");
                IsConnected = false; //TODO, does cassandra have auto retry connect?
                return null;
            }                        
        }
        
        protected async Task<T> MapperAsync<T>(Func<IMapper, string, object[], Task<T>> func, string query, params object[] args)
        where T:class
        {
            if (!await EnsureDatabaseAsync().ConfigureAwait(CfgAwait))
                return null;
           
            try
            {
                return await func(_mapper, query, args).ConfigureAwait(CfgAwait);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"`{BindQueryParameters(query, args)}'");
                IsConnected = false;
                return null;
            }
        }

        /// <summary>
        /// Returns a query string with bound parameters
        /// </summary>
        /// <param name="query">The query</param>
        /// <param name="args">The parameters to bind</param>
        /// <returns>A query string containing parameters</returns>
        protected string BindQueryParameters(string query, params object[] args)
        {
            foreach (var o in args)
            {
                if (o.GetType().IsArray)
                {
                    var setStrChrArray = (new ArrayList((Array) o)).ToArray().SelectMany(e => $"{e},").ToList();
                    var setString = Encoding.ASCII.GetString(setStrChrArray.ConvertAll(input => (byte)input).ToArray()).TrimEnd(',');                    
                    query = query.ReplaceFirst("?", $"({setString})");
                }
                else
                {
                    query = query.ReplaceFirst("?", $"{o}");
                }                
            }
            return query;
        }
    }
}
