using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using NLog;

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
        /// <param name="url">The node url in the form tcp://HOST:port or udp://HOST:port</param>
        public IoNodeAddress(string url)
        {
            try
            {
                Url = url;

                var uriAndIpAndPort = Url.Split(":");
                var uriAndIp = uriAndIpAndPort[0] + ":" + uriAndIpAndPort[1];

                Port = int.Parse(uriAndIpAndPort[2]);
                HostStr = StripIpFromUrlString(uriAndIp);                
            }
            catch (Exception e)
            {
                Validated = false;
                ValidationErrorString = $"Unable to parse {url}, must be in the form tcp://IP:port or udp://IP:port. ({e.Message})";
                return;
            }

            _logger = LogManager.GetCurrentClassLogger();
        }

        private Logger _logger;

        [DataMember]
        public string Url { get; }

        [DataMember]
        public string Key => $"{HostStr}:{Port}";

        [DataMember]
        public IPAddress ResolvedIpAddress { get; protected set; }
        
        /// <summary>
        /// The listening port of the remote node
        /// </summary>
        [IgnoreDataMember]
        public int Port { get; protected set; }

        /// <summary>
        /// <see cref="IoNodeAddress"/> wrapped as <see cref="System.Net.IPEndPoint"/>
        /// </summary>
        [IgnoreDataMember]
        public IPEndPoint IpEndPoint { get; protected set; }

        [DataMember]
        public IPEndPoint ResolvedIpEndPoint { get; protected set; }

        /// <summary>
        /// The Ip
        /// </summary>
        [IgnoreDataMember]
        public string HostStr { get; protected set; }

        [IgnoreDataMember]
        public string ProtocolDesc { get; protected set; }

        /// <summary>
        /// Returns true if the URL format is valid.
        /// </summary>
        [IgnoreDataMember]
        public bool Validated { get; protected set; }

        /// <summary>
        /// Returns true if dns resolved
        /// </summary>
        [IgnoreDataMember]
        public bool DnsValidated { get; protected set; } = false;

        /// <summary>
        /// Returns true if the dns resolution has changed
        /// </summary>
        [IgnoreDataMember]
        public bool DnsResolutionChanged { get; protected set; } = false;

        /// <summary>
        /// The validation error string detailing validation errors
        /// </summary>
        [IgnoreDataMember]
        public string ValidationErrorString { get; protected set; }

        /// <summary>
        /// Returns the address as ip:port
        /// </summary>
        [IgnoreDataMember]
        public string IpAndPort => $"{HostStr}:{Port}";

        [IgnoreDataMember]
        public string ResolvedIpAndPort => $"{IpEndPoint?.Address}:{IpEndPoint?.Port}";
        
        /// <summary>
        /// Creates a new node address descriptor
        /// </summary>
        /// <param name="url">The node url in the form tcp:// or udp://</param>
        /// <param name="port">The node listening port</param>
        /// <returns></returns>
        public static IoNodeAddress Create(string url)
        {
            var address = new IoNodeAddress(url);
            address.Validate(); //TODO move this closer to where it is needed
            return address;
        }

        /// <summary>
        /// The Url string in form url://ip:port
        /// </summary>
        /// <returns>The Url string in form url://ip:port</returns>
        public override string ToString()
        {
            return Url;
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
            {
                ProtocolDesc = "tcp://";
                return ProtocolType.Tcp;
            }


            if (Url.Contains("udp://"))
            {
                ProtocolDesc = "udp://";
                return ProtocolType.Udp;
            }
                
            return ProtocolType.Unknown;
        }

        /// <summary>
        /// Validates syntax and DNS
        /// </summary>
        /// <returns>True on validated</returns>
        public bool Validate()
        {
            try
            {
                if (string.IsNullOrEmpty(HostStr))
                {
                    var uriAndIpAndPort = Url.Split(":");
                    var uriAndIp = uriAndIpAndPort[0] + ":" + uriAndIpAndPort[1];

                    Port = int.Parse(uriAndIpAndPort[2]);
                    HostStr = StripIpFromUrlString(uriAndIp);
                }

                Validated = true;

                Resolve();
            }
            catch (Exception e)
            {
                Validated = false;
                ValidationErrorString = $"Unable to parse {Url}, must be in the form tcp://IP:port or udp://IP:port. ({e.Message})";
                return false;
            }

            return Validated || !DnsResolutionChanged;
        }

        /// <summary>
        /// Resolves Dns, <see cref="DnsValidated"/> will be set to true
        /// </summary>
        private void Resolve()
        {
            try
            {
                if (HostStr == "0.0.0.0")
                {
                    IpEndPoint = new IPEndPoint(0, Port);
                    DnsResolutionChanged = false;
                    DnsValidated = true;
                    return;
                }
                
                var resolvedIpAddress = Dns.GetHostAddresses(HostStr)[0];
                if (!IpEndPoint?.Address.Equals(resolvedIpAddress) ?? false)
                {
                    ResolvedIpEndPoint = IpEndPoint;
                    IpEndPoint = new IPEndPoint(resolvedIpAddress, Port);
                    DnsResolutionChanged = true;
                }
                else
                {
                    IpEndPoint = new IPEndPoint(resolvedIpAddress, Port);
                    DnsResolutionChanged = false;
                    DnsValidated = true;
                }                
            }
            catch (Exception e)
            {
                Validated = false;
                DnsValidated = false;
                _logger.Error(e,$"Unable to resolve host name for `{Url}':");
            }
        }

        public static IoNodeAddress CreateFromRemoteSocket(Socket socket)
        {
            if (socket.ProtocolType == ProtocolType.Tcp)
            {
                return new IoNodeAddress($"tcp://{socket.RemoteAddress()}:{socket.RemotePort()}");
            }
            else
            {
                throw new NotSupportedException("Only TCP supports IoNodeAddress from remote sockets!");
            }            
        }

        public static IoNodeAddress CreateFromEndpoint(EndPoint udpRemoteEndpointInfo)
        {
            return new IoNodeAddress($"udp://{((IPEndPoint)udpRemoteEndpointInfo).Address}:{((IPEndPoint)udpRemoteEndpointInfo).Port}");
        }
    }
}
