using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using zero.core.data.contracts;
using zero.core.data.providers.redis;
using zero.core.models;
using zero.core.network.ip;
using zero.interop.entangled.common.model.interop;
using zero.interop.utils;
using zero.tangle.entangled.common.model;

namespace zero.tangle.data.redis.configurations.tangle
{
    public class IoTangleTransactionCache<TBlob>: IoRedisBase, IIoDataSource<bool>    
    {

        private static volatile IoTangleTransactionCache<TBlob> _default = new IoTangleTransactionCache<TBlob>();
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>

        public static async Task<IoTangleTransactionCache<TBlob>> Default()
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
            var tangleTransaction = (IIoTransactionModel<TBlob>) transaction;
            return _db.StringSetAsync(Encoding.UTF8.GetString(tangleTransaction.HashBuffer.Span), tangleTransaction.AsBlob().AsArray());
        }

        public async Task<TTransaction> GetAsync<TTransaction,TBlobF>(TBlobF key)
            where TTransaction : class, IIoTransactionModelInterface
        {
            RedisValue val;
            if (key is string)
                val = await _db.StringGetAsync(key as string);            
            else
                val = await _db.StringGetAsync(Encoding.UTF8.GetString(key as byte[]));


            if (val == RedisValue.Null)
                return default(TTransaction);
                        
            return  new IoInteropTransactionModel
            {
                Blob = (byte[])val
            } as TTransaction;
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
