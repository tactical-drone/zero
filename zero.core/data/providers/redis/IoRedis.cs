﻿using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using zero.core.data.contracts;
using zero.core.network.ip;
using zero.interop.entangled.common.model.interop;
using zero.interop.utils;

namespace zero.core.data.providers.redis
{
    public class IoRedis: IoRedisBase, IIoDataSource<bool>    
    {

        private static volatile IoRedis _default = new IoRedis();
        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>
#pragma warning disable 1998
        public static async Task<IoRedis> Default()
#pragma warning restore 1998
        {
            if(!_default.IsConnected)            
#pragma warning disable 4014
                _default.ConnectAsync(new []{IoNodeAddress.Create("tcp://10.0.75.1:6379") }.ToList()); //TODO config
#pragma warning restore 4014

            return _default;
        }

        public bool IsConnected => _redis?.IsConnected??false;
        public Task<bool> Put<TBlob>(IIoTransactionModel<TBlob> transaction, object userData = null)
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

        public Task<bool> Exists<TBlob>(TBlob key)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExecuteAsync(object batch)
        {
            throw new NotImplementedException();
        }
    }
}