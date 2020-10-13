using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

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
        ///// Used by listeners
        ///// </summary>
        ///// <param name="proxy">The proxy to steel a <see cref="System.Net.Sockets.Socket"/> from</param>
        ///// <param name="endPoint"></param>
        //public IoUdpClient(IoNetClient<TJob> proxy, IPEndPoint endPoint) : base(new IoUdpSocket(proxy.IoNetSocket.NativeSocket, endPoint), proxy.PrefetchSize, proxy.ConcurrencyLevel)
        //{
            
        //}

        public IoUdpClient(int readAheadBufferSize, int concurrencyLevel) : base(readAheadBufferSize, concurrencyLevel)
        {

        }

        public IoUdpClient(IoSocket ioSocket, int readAheadBufferSize, int concurrencyLevel) : base(new IoUdpSocket(ioSocket.NativeSocket, new IPEndPoint(IPAddress.Any, 305), concurrencyLevel), readAheadBufferSize, concurrencyLevel)
        {
            
        }


        readonly byte[] _blacklist;

        public IoUdpClient(IoNetClient<TJob> clone, IPEndPoint newRemoteEp) : base(new IoUdpSocket(clone.IoNetSocket.NativeSocket, newRemoteEp, clone.ConcurrencyLevel), clone.PrefetchSize, clone.ConcurrencyLevel)
        {

        }

        /// <summary>
        /// current blacklist
        /// </summary>
        public byte[] BlackList => _blacklist;

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress)
        {
            IoNetSocket = ZeroOnCascade(new IoUdpSocket(ConcurrencyLevel), true).target;
            return await base.ConnectAsync(remoteAddress).ConfigureAwait(false);
        }

        public override void Blacklist(int remoteAddressPort)
        {
            _blacklist[remoteAddressPort] = 0;
        }

        public override void WhiteList(int remoteAddressPort)
        {
            _blacklist[remoteAddressPort] = 0;
        }
    }
}
