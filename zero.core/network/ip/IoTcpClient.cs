﻿using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <summary>
    /// The <see cref="IoNetClient{TJob}"/>'s TCP flavor, produces tcp packets for <see cref="IIoZero"/>
    /// </summary>
    /// <seealso cref="IoNetClient{TJob}" />
    public class IoTcpClient<TJob> : IoNetClient<TJob>
        where TJob : IIoJob

    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class.
        /// </summary>
        /// <param name="description">Description</param>
        /// <param name="remote">The tcp client to be wrapped</param>
        /// <param name="prefetchSize">The amount of socket reads the source is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoTcpClient(string description, IoNetSocket remote, int prefetchSize = 1,  int concurrencyLevel = 1) : base(description, remote, prefetchSize,  concurrencyLevel, false)
        {
            LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class. Used in combination with <see cref="ConnectAsync"/>
        /// </summary>
        /// <param name="description">Description</param>
        /// <param name="prefetchSize">The amount of socket reads the source is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoTcpClient(string description, int prefetchSize,  int concurrencyLevel) : base(description, prefetchSize,  concurrencyLevel)
        {
            LogManager.GetCurrentClassLogger();
        }
        
        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout)
        {
            IoNetSocket = (await ZeroHiveAsync(new IoTcpSocket(ZeroConcurrencyLevel), true).FastPath()).target;
            return await base.ConnectAsync(remoteAddress, timeout).FastPath();
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
#endif
        }
    }
}
