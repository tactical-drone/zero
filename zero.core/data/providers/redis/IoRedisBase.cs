using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using zero.core.network.ip;
using StackExchange.Redis;
using zero.core.conf;

namespace zero.core.data.providers.redis
{
    /// <summary>
    /// Redis data source common functionality
    /// </summary>
    public class IoRedisBase : IoConfigurable
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoRedisBase()
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
        public async Task<bool> ConnectAsync(List<IoNodeAddress> hosts)
        {
            //Used for now to ignore missing redis hosts
            if (_connectionAttempts >= MaxConnectionAttempts) //TODO remove for production
                return false;

            _hosts = hosts;
            var hostsStringDesc = new string((hosts.ToList().Select(u => u.IpAndPort + ",").SelectMany(u => u).ToArray())).Trim(',');

            _logger.Debug($"Connecting to redis at `{hostsStringDesc}'");

            try
            {
                _connectionAttempts++;
                _redis = await ConnectionMultiplexer.ConnectAsync(hostsStringDesc);
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
        public async Task<bool> EnsureConnectionAsync()
        {
            if (IsConnected)
                return true;

            if (_redis != null)
            {
                await _redis.CloseAsync();
                _redis = null;
                _db = null;
            }
                
            return await ConnectAsync(_hosts);
        }
    }
}
