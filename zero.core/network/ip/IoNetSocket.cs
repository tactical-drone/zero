using System.Net.Sockets;
using System.Threading;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// Marks the more generic <see cref="IoSocket"/> for use in our abstraction
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoSocket" />
    /// <seealso cref="IIoProducer" />
    public abstract class IoNetSocket : IoSocket, IIoProducer
    {
        protected IoNetSocket(SocketType socketType, ProtocolType protocolType, CancellationToken cancellationToken) : base(socketType, protocolType, cancellationToken)
        {
        }

        protected IoNetSocket(Socket socket, IoNodeAddress listenerAddress, CancellationToken cancellationToken) : base(socket, listenerAddress, cancellationToken)
        {
        }        
    }
}
