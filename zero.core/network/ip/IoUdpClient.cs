﻿using System.Net;
using System.Threading.Tasks;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <summary>
    /// The UDP flavor of <see cref="IoNetClient{TJob}"/>
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoNetClient{TJob}" />
    public class IoUdpClient<TJob> : IoNetClient<TJob>
        where TJob : IIoJob

    {
        /// <summary>
        /// Base constructor
        /// </summary>
        /// <param name="description"></param>
        /// <param name="readAheadBufferSize">Read ahead size</param>
        /// <param name="concurrencyLevel">Level of concurrency</param>
        public IoUdpClient(string description, int readAheadBufferSize, int concurrencyLevel) : base(description, readAheadBufferSize, concurrencyLevel)
        {

        }

        /// <summary>
        /// Constructor used by listeners
        /// </summary>
        /// <param name="description"></param>
        /// <param name="ioSocket">The socket the listener created</param>
        /// <param name="prefetch">Read ahead size</param>
        /// <param name="concurrencyLevel">Level of concurrency</param>
        public IoUdpClient(string description, IoSocket ioSocket, int prefetch, int concurrencyLevel) : base(description, new IoUdpSocket(ioSocket.NativeSocket, new IPEndPoint(IPAddress.Any, 305), concurrencyLevel), prefetch, concurrencyLevel)
        {
            
        }

        /// <summary>
        /// Constructor used to create a <see cref="IoNetClient{TJob}"/> that wraps <see cref="clone"/>'s native UDP socket.
        /// 
        /// We call these connections, Proxies. But they are not really proxies. This is just a hack to get UDP
        /// multi-connectionless setup that listens on the same port, to work. The correct way to do this would be
        /// to have one local listening port for each remote client. But, some applications don't do it that way. Hence support
        /// is added here for such setups.
        /// 
        /// </summary>
        /// <param name="description"></param>
        /// <param name="clone"></param>
        /// <param name="newRemoteEp"></param>
        public IoUdpClient(string description, IoNetClient<TJob> clone, IPEndPoint newRemoteEp) : base(description , new IoUdpSocket(clone.IoNetSocket.NativeSocket, newRemoteEp, clone.PrefetchSize, true), clone.PrefetchSize, clone.PrefetchSize, true)
        {
            
        }

        /// <summary>
        /// current blacklist
        /// </summary>
        public byte[] BlackList { get; } = null;

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout)
        {
            IoNetSocket = (await ZeroHiveAsync(new IoUdpSocket(ZeroConcurrencyLevel()), true).FastPath().ConfigureAwait(Zc)).target;
            return await base.ConnectAsync(remoteAddress, timeout).FastPath().ConfigureAwait(Zc);
        }

        public override void Blacklist(int remoteAddressPort)
        {
            //_blacklist[remoteAddressPort] = 0;
        }

        public override void WhiteList(int remoteAddressPort)
        {
            //_blacklist[remoteAddressPort] = 0;
        }
    }
}
