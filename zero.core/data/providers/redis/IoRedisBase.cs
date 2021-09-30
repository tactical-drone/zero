using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using zero.core.network.ip;
using StackExchange.Redis;
using zero.core.conf;
using zero.core.patterns.misc;

namespace zero.core.data.providers.redis
{
    /// <summary>
    /// Redis data source common functionality
    /// </summary>
    public class IoRedisBase : IoNanoprobe
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoRedisBase() : base($"{nameof(IoRedisBase)}")
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The class logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Redis connection multiplexer
        /// </summary>
        protected ConnectionMultiplexer _redis;

        /// <summary>
        /// Redis database handle
        /// </summary>
        protected IDatabase _db;

        /// <summary>
        /// The list of hosts connected to
        /// </summary>
        protected List<IoNodeAddress> _hosts;

        /// <summary>
        /// Returns true if the connection is up
        /// </summary>
        public bool IsConnected => _redis?.IsConnected ?? false;

        /// <summary>
        /// Used to passively ignore missing redis configurations
        /// </summary>
        private int _connectionAttempts = 0;

        /// <summary>
        /// Maximum amount of connection retries before giving up
        /// </summary>
        private const int MaxConnectionAttempts = 1;

        /// <summary>
        /// Connect to redis hosts
        /// </summary>
        /// <param name="hosts">A list of hosts to connect to</param>
        /// <returns>True if connected, false otherwise</returns>
        public async ValueTask<bool> ConnectAsync(List<IoNodeAddress> hosts)
        {
            //Used for now to ignore missing redis hosts
            if (_connectionAttempts >= MaxConnectionAttempts) //TODO remove for production
                return false;

            _hosts = hosts;
            var hostsStringDesc = new string((hosts.ToList().Select(u => u.Ip + ",").SelectMany(u => u).ToArray())).Trim(',');

            _logger.Trace($"Connecting to redis at `{hostsStringDesc}'");

            try
            {
                _connectionAttempts++;
                _redis = await ConnectionMultiplexer.ConnectAsync(hostsStringDesc).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Failed to connect to redis at `{hostsStringDesc}':");
                return false;
            }
            _logger.Info($"Connected to redis: `{hostsStringDesc}'");
            _connectionAttempts = 0;

            _db = _redis.GetDatabase();
            return true;
        }

        /// <summary>
        /// Checks whether redis is up
        /// </summary>
        /// <returns>true if it is up</returns>
        public async ValueTask<bool> EnsureConnectionAsync()
        {
            if (IsConnected)
                return true;

            if (_redis != null)
            {
                await _redis.CloseAsync().ConfigureAwait(false);
                _redis = null;
                _db = null;
            }
                
            return await ConnectAsync(_hosts).ConfigureAwait(false);
        }
    }
}
