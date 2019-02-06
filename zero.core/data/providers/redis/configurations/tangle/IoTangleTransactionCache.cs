using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using zero.core.data.contracts;
using zero.core.network.ip;
using zero.interop.entangled.common.model.interop;
using zero.interop.utils;

namespace zero.core.data.providers.redis.configurations.tangle
{
    public class IoTangleTransactionCache: IoRedisBase, IIoDataSource<bool>    
    {

        private static volatile IoTangleTransactionCache _default = new IoTangleTransactionCache();
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>

        public static async Task<IoTangleTransactionCache> Default()
        {
            if(!_default.IsConnected)            
                await _default.ConnectAsync(new []{IoNodeAddress.Create("tcp://10.0.75.1:6379") }.ToList()); //TODO config

            return _default;
        }

        /// <summary>
        /// Returns true if redis is connected, false otherwise
        /// </summary>
        public new bool IsConnected => base.IsConnected;

        public Task<bool> PutAsync<TBlob>(IIoTransactionModel<TBlob> transaction, object userData = null)
        {            
            return _db.StringSetAsync(Encoding.UTF8.GetString(transaction.HashBuffer.Span), transaction.AsBlob().AsArray());
        }

        public async Task<IIoTransactionModel<TBlob>> Get<TBlob>(ReadOnlyMemory<byte> txKey)
        {            
            RedisValue val = await _db.StringGetAsync(Encoding.UTF8.GetString(txKey.Span));

            if (val == RedisValue.Null)
                return null;
            
            //TODO support native
            return (IIoTransactionModel<TBlob>) new IoInteropTransactionModel
            {
                Blob = (byte[])val
            };
        }

        public Task<bool> TransactionExistsAsync<TBlob>(TBlob key)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExecuteAsync(object usedData)
        {
            throw new NotImplementedException();
        }
    }
}
