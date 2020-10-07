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
        /// Initializes a new instance of the <see cref="IoUdpClient{TJob}"/> class.
        /// 
        /// Used by listener
        /// </summary>
        /// <param name="remote">The udp client to be wrapped</param>
        /// <param name="prefetchSize">The amount of socket reads the producer is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        /// <param name="fromAddress">From address</param>
        public IoUdpClient(IoSocket remote, int prefetchSize, int concurrencyLevel, IoNodeAddress fromAddress = null) : base((IoNetSocket)remote, prefetchSize,  concurrencyLevel)
        {
            IoSocket = ZeroOnCascade(new IoUdpSocket(remote.NativeSocket, remote.ListeningAddress, fromAddress), true).target;

            if(fromAddress == null)
                _blacklist = new byte[ushort.MaxValue];
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoUdpClient{TJob}"/> class.
        /// 
        /// Needed to connect
        /// </summary>
        /// <param name="localAddress">The address associated with this network client</param>
        /// <param name="prefetchSize">The amount of socket reads the producer is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        public IoUdpClient(IoNodeAddress localAddress, int prefetchSize,  int concurrencyLevel) : base(localAddress, prefetchSize,  concurrencyLevel)
        {
            //_blacklist = new byte[ushort.MaxValue];
        }

        readonly byte[] _blacklist;

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
        public override async Task<bool> ConnectAsync()
        {
            (IoSocket,_) = ZeroOnCascade(new IoUdpSocket(), true);
            return await base.ConnectAsync().ConfigureAwait(false);
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
