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

        /// <summary>
        /// Description
        /// </summary>
        private string _description;

        /// <summary>
        /// Description
        /// </summary>
        public override string Description
        {
            get
            {
                if (_description == null && (ListeningAddress != null || LocalEndPoint != null))
                {
                    if (Kind == Connection.Listener)
                        return _description = $"`listener: {ListeningAddress}'";
                    else
                        return _description = $"`socket: {Key}'";
                }
                
                return _description;
            }
        } 
    }
}
