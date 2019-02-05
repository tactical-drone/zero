using System;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using zero.core.data.contracts;
using zero.core.network.ip;

namespace zero.core.data.providers.redis
{
    public class IoRedisDupChecker : IoRedisBase, IIoDupChecker
    {
        private static volatile IoRedisDupChecker _default = new IoRedisDupChecker();
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>

        public static async Task<IoRedisDupChecker> Default()
        {
            if (!_default.IsConnected)
                await _default.ConnectAsync(new[] { IoNodeAddress.Create("tcp://10.0.75.1:6379") }.ToList()); //TODO config

            return _default;
        }

        private static readonly double DupCheckWindowInHours = 2;

        //TODO redis does not like a pre baked Timespan?
        public TimeSpan DupCheckWindow { get; } = TimeSpan.FromHours(DupCheckWindowInHours);

        public bool IsConnected => _redis?.IsConnected ?? false;

        public async Task<bool> IsDuplicate(string key)
        {
            return !await _db.StringSetAsync(key, 0, TimeSpan.FromHours(DupCheckWindowInHours), When.NotExists);
        }
    }
}
