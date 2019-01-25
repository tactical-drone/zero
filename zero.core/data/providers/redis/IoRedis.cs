using System;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using StackExchange.Redis;
using zero.core.data.contracts;
using zero.core.data.providers.cassandra;
using zero.core.network.ip;
using zero.interop.entangled;
using zero.interop.entangled.common.model.interop;

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
                await _default.ConnectAsync(new []{IoNodeAddress.Create("tcp://10.0.75.1:6379") }.ToList()); //TODO config
#pragma warning restore 4014

            return _default;
        }

        public bool IsConnected => _redis?.IsConnected??false;
        public Task<bool> Put<TBlob>(IIoTransactionModel<TBlob> transaction, object batch = null)
        {            
            MemoryMarshal.TryGetString((char[])(Array)transaction.HashBuffer.ToArray(), out var key, out var start, out var length);
            return _db.StringSetAsync(key, transaction.AsBlob().ToArray());
        }

        public async Task<IIoTransactionModel<TBlob>> Get<TBlob>(ReadOnlyMemory<byte> txKey)
        {
            MemoryMarshal.TryGetString((char[])(Array)txKey.ToArray(), out var key, out var start, out var length);

            RedisValue val = await _db.StringGetAsync(key);

            if (val == RedisValue.Null)
                return null;
            
            //TODO bug
            return (IIoTransactionModel<TBlob>) new IoInteropTransactionModel
            {
                Blob = (byte[])(Array)((string)val).AsMemory().Slice(0).ToArray()
            };
        }

        public Task<bool> ExecuteAsync(object batch)
        {
            throw new NotImplementedException();
        }
    }
}
