using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using zero.core.network.ip;
using StackExchange.Redis;

namespace zero.core.data.providers.redis
{
    /// <summary>
    /// Redis data source
    /// </summary>
    public class IoRedisBase
    {
        public IoRedisBase()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The class logger
        /// </summary>
        private readonly Logger _logger;

        protected ConnectionMultiplexer _redis;

        protected IDatabase _db;

        public async Task<bool> ConnectAsync(List<IoNodeAddress> url)
        {
            var hosts = new string((url.ToList().Select(u => u.IpAndPort + ":").SelectMany(u => u).ToArray())).Trim(':');
            _logger.Info($"Connecting to redis: `{hosts}'");
            try
            {
                _redis = await ConnectionMultiplexer.ConnectAsync(hosts);
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Failed to connect to redis at `{hosts}':");
                return false;
            }
            _db = _redis.GetDatabase();
            return true;
        }
    }
}
