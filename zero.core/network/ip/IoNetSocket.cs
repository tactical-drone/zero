using System.Net.Sockets;
using System.Threading;
using zero.core.data.contracts;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// Marks the more generic <see cref="IoSocket"/> for use in our abstraction
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoSocket" />
    /// <seealso cref="IIoSource" />
    public abstract class IoNetSocket : IoSocket, IIoSourceBase
    {
        protected IoNetSocket(SocketType socketType, ProtocolType protocolType) : base(socketType, protocolType)
        {
        }

        protected IoNetSocket(Socket socket, IoNodeAddress listeningAddress) : base(socket, listeningAddress)
        {
        }

        public string Description => $"{this.GetType().Name} `{base.LocalAddress}'";
        public string SourceUri => $"{base.ListeningAddress}";
        public bool IsOperational => NativeSocket.Connected;
        public IIoDupChecker RecentlyProcessed { get; set; }
    }
}
