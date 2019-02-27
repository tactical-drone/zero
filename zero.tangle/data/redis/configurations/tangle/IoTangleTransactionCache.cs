using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using zero.core.data.contracts;
using zero.core.data.providers.redis;
using zero.core.models;
using zero.core.network.ip;
using zero.interop.utils;
using zero.tangle.entangled.common.model;
using zero.tangle.models;

namespace zero.tangle.data.redis.configurations.tangle
{
    public class IoTangleTransactionCache<TKey>: IoRedisBase, IIoDataSource<bool>    
    {

        private static volatile IoTangleTransactionCache<TKey> _default = new IoTangleTransactionCache<TKey>();
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>

        public static async Task<IoTangleTransactionCache<TKey>> Default()
        {
            if(!_default.IsConnected)            
                await _default.ConnectAsync(new []{IoNodeAddress.Create("tcp://10.0.75.1:6379") }.ToList()); //TODO config

            return _default;
        }

        /// <summary>
        /// Returns true if redis is connected, false otherwise
        /// </summary>
        public new bool IsConnected => base.IsConnected;

        public Task<bool> PutAsync<TTransaction>(TTransaction transaction, object userData = null)
            where TTransaction : class, IIoTransactionModelInterface
        {
            var tangleTransaction = (IIoTransactionModel<TKey>) transaction;
            return _db.StringSetAsync(Encoding.UTF8.GetString(tangleTransaction.HashBuffer.Span), tangleTransaction.AsBlob().AsArray());
        }

        public async Task<TTransaction> GetAsync<TTransaction,TKeyF>(TKeyF key)
            where TTransaction : class, IIoTransactionModelInterface
        {
            RedisValue val;
            if (key is string)
                val = await _db.StringGetAsync(key as string);            
            else
                val = await _db.StringGetAsync(Encoding.UTF8.GetString(key as byte[]));


            if (val == RedisValue.Null)
                return default(TTransaction);
                        
            return  new EntangledTransaction
            {
                Blob = (byte[])val
            } as TTransaction;
        }

        public Task<bool> TransactionExistsAsync<TKeyF>(TKeyF key)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExecuteAsync(object usedData)
        {
            throw new NotImplementedException();
        }
    }
}
