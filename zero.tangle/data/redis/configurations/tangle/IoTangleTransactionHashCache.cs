using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using zero.core.conf;
using zero.core.data.contracts;
using zero.core.data.providers.redis;
using zero.core.network.ip;

namespace zero.tangle.data.redis.configurations.tangle
{
    public class IoTangleTransactionHashCache : IoRedisBase, IIoDupChecker
    {
        /// <summary>
        /// Redis reentrant connection handle
        /// </summary>
        private static volatile IoTangleTransactionHashCache _default = new IoTangleTransactionHashCache();

        [IoParameter]
        public string parm_redis_tangle_tx_hash_cache_url = "tcp://10.0.75.1:6379";

        /// <summary>
        /// Returns single thread safe connection
        /// </summary>
        /// <returns></returns>
        public static async Task<IoTangleTransactionHashCache> Default()
        {
            var hosts = new List<IoNodeAddress>();
            _default.parm_redis_tangle_tx_hash_cache_url.Split(',').ToList().ForEach(address=>hosts.Add(IoNodeAddress.Create(address)));

            if (!_default.IsConnected)
                await _default.ConnectAsync(hosts);

            return _default;
        }

        /// <summary>
        /// The TTL of keys
        /// </summary>
        private static readonly double DupCheckWindowInHours = 2;

        //TODO redis does not like a pre baked Timespan?
        public TimeSpan DupCheckWindow { get; } = TimeSpan.FromHours(DupCheckWindowInHours);

        /// <summary>
        /// Check if a key exists in the db
        /// </summary>
        /// <param name="key">The key</param>
        /// <returns>true if it exists, false otherwise</returns>
        public async Task<bool> KeyExistsAsync(string key)
        {
            if( await EnsureConnectionAsync() )
                return !await _db.StringSetAsync(key, 0, TimeSpan.FromHours(DupCheckWindowInHours), When.NotExists);
            return false;
        }

        /// <summary>
        /// Removes a key from the db
        /// </summary>
        /// <param name="key">The key to be removed</param>
        /// <returns>true if they key was removed, false otherwise</returns>
        public async Task<bool> DeleteKeyAsync(string key)
        {
            if(await EnsureConnectionAsync())
                return await _db.KeyDeleteAsync(key, CommandFlags.HighPriority);
            return false;
        }
    }
}
