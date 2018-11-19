using System;
using System.Net;
using System.Net.Sockets;

namespace zero.core.network.ip
{
    /// <summary>
    /// Used to store address information of remote nodes
    /// </summary>
    public class IoNodeAddress
    {
        /// <summary>
        /// Constructs a new node address
        /// </summary>
        /// <param name="url">The node url in the form tcp:// or udp://</param>
        /// <param name="port">The node listening port</param>
        public IoNodeAddress(string url, int port)
        {
            Url = url;
            Port = port;
            Ip = StripIpFromUrlString(Url);

            //IpEndPoint = new IPEndPoint(IPAddress.Parse(StripIpFromUrlString(Url)), Port);
            IpEndPoint = new IPEndPoint(Dns.GetHostAddresses(StripIpFromUrlString(Url))[0], Port);
        }

        /// <summary>
        /// The address url of the node
        /// </summary>
        public string Url;

        /// <summary>
        /// The listening port of the remote node
        /// </summary>
        public int Port;

        /// <summary>
        /// <see cref="IoNodeAddress"/> wrapped as <see cref="System.Net.IPEndPoint"/>
        /// </summary>
        public IPEndPoint IpEndPoint;

        /// <summary>
        /// The Ip
        /// </summary>
        public string Ip;

        /// <summary>
        /// Returns the address as ip:port
        /// </summary>
        public string IpAndPort => $"{Ip}:{Port}";

        /// <summary>
        /// Returns the address in the format url:port
        /// </summary>
        public string UrlAndPort => $"{Url}:{Port}";

        /// <summary>
        /// Creates a new node address descriptor
        /// </summary>
        /// <param name="url">The node url in the form tcp:// or udp://</param>
        /// <param name="port">The node listening port</param>
        /// <returns></returns>
        public static IoNodeAddress Create(string url, int port)
        {
            return new IoNodeAddress(url, port);
        }

        /// <summary>
        /// The Url string in form url://ip:port
        /// </summary>
        /// <returns>The Url string in form url://ip:port</returns>
        public override string ToString()
        {
            return UrlAndPort;
        }

        /// <summary>
        /// Strips the IP from a URL string
        /// </summary>
        /// <param name="url">The url to be stripped</param>
        /// <returns>The ip contained in the url</returns>
        public static string StripIpFromUrlString(string url)
        {
            if (!url.Contains("tcp://") && !url.Contains("udp://"))
                throw new ArgumentException($"Url string must be in the format tcp://IP:PORT or udp://IP:PORT");

            return url.Replace("tcp://", "").Replace("udp://", "").Split(":")[0];
        }

        /// <summary>
        /// Returns the <see cref="ProtocolType"/>
        /// </summary>
        /// <returns><see cref="ProtocolType.Tcp"/> if protocol tcp, <see cref="ProtocolType.Udp"/> if udp</returns>
        public ProtocolType Protocol()
        {
            if (Url.Contains("tcp://"))
                return ProtocolType.Tcp;

            if (Url.Contains("udp://"))
                return ProtocolType.Udp;

            return ProtocolType.Unknown;
        }
    }
}
