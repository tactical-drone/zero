using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;

namespace zero.core.network.ip
{
    /// <summary>
    /// Used to store address information of remote nodes
    /// </summary>
    [DataContract]  
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

            try
            {
                Ip = StripIpFromUrlString(Url);
            }
            catch (ArgumentException e)
            {
                IsValid = false;
                ValidationErrorString = e.Message;
                return;
            }

            IpEndPoint = new IPEndPoint(Dns.GetHostAddresses(StripIpFromUrlString(Url))[0], Port);
        }

        /// <summary>
        /// The address url of the node
        /// </summary>
        [DataMember]
        public string Url;

        /// <summary>
        /// The listening port of the remote node
        /// </summary>
        [DataMember]
        public int Port;

        /// <summary>
        /// <see cref="IoNodeAddress"/> wrapped as <see cref="System.Net.IPEndPoint"/>
        /// </summary>
        [IgnoreDataMember]
        public IPEndPoint IpEndPoint;

        /// <summary>
        /// The Ip
        /// </summary>
        [IgnoreDataMember]
        public string Ip;

        /// <summary>
        /// Returns true if the URL format is valid.
        /// </summary>
        [IgnoreDataMember]
        public bool IsValid = false;

        /// <summary>
        /// The validation error string detailing validation errors
        /// </summary>
        public string ValidationErrorString = null;

        /// <summary>
        /// Returns the address as ip:port
        /// </summary>
        [IgnoreDataMember]
        public string IpAndPort => $"{Ip}:{Port}";

        /// <summary>
        /// Returns the address in the format url:port
        /// </summary>
        [IgnoreDataMember]
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
