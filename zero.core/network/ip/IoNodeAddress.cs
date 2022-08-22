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
        /// private constructor
        /// </summary>
        private IoNodeAddress()
        {

        }
        /// <summary>
        /// Constructs a new node address
        /// </summary>
        /// <param name="url">The node url in the form tcp://HOST:port or udp://HOST:port</param>
        /// <param name="lookup">Whether to perform a dns lookup on <see cref="Ip"/></param>
        public IoNodeAddress(string url, bool lookup = false)
        {
            _performDns = lookup;
            Init(url);
            IpEndPoint = new IPEndPoint(IPAddress.Parse(Ip),Port);
        }

        /// <summary>
        /// Constructor from endpoint
        /// </summary>
        /// <param name="url">The url</param>
        /// <param name="endpoint">The endpoint</param>
        private IoNodeAddress(string url, IPEndPoint endpoint):this(url)
        {
            IpEndPoint = endpoint;
        }

        /// <summary>
        /// The logger
        /// </summary>
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        [DataMember]
        public string Url { get; set; }

        [DataMember] public string Key => $"{ProtocolDesc}{IpEndPoint}";

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
        public IPEndPoint IpEndPoint { get; private set; }

        [DataMember]
        public IPEndPoint ResolvedIpEndPoint { get; protected set; }

        /// <summary>
        /// Whether to perform a DNS lookup
        /// </summary>
        private readonly bool _performDns;

        /// <summary>
        /// The Ip
        /// </summary>
        [IgnoreDataMember]
        public string Ip { get; protected set; }

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
        public string IpPort { get; private set; }

        [IgnoreDataMember]
        public string EndpointIpPort => $"{IpEndPoint?.Address}:{IpEndPoint?.Port}";

        /// <summary>
        /// Initializes the data from a url string
        /// </summary>
        /// <param name="url">The url</param>
        private void Init(string url)
        {
            Url = url;
            Parse();

            if(_performDns)
                Resolve();
        }
        
        /// <summary>
        /// Creates a new node address descriptor
        /// </summary>
        /// <param name="url">The node url in the form tcp:// or udp://</param>
        /// <returns></returns>
        public static IoNodeAddress Create(string url)
        {
            return new IoNodeAddress(url);
        }

        /// <summary>
        /// Mutate the state, UDP needs this
        /// </summary>
        /// <param name="endpoint">The new endpoint</param>
        /// <returns>The modified address</returns>
        public IoNodeAddress Update(IPEndPoint endpoint)
        {
            if (endpoint.Address.Equals(IpEndPoint.Address) && endpoint.Port == Port)
                return this;

            Ip = endpoint.Address.ToString();
            Port = endpoint.Port;
            IpEndPoint = endpoint;
            Url = $"{ProtocolDesc}{IpPort}";

            return this;
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

            ProtocolDesc = "zero://";
            return ProtocolType.Unknown;
        }

        /// <summary>
        /// Validates syntax and DNS
        /// </summary>
        /// <returns>True on validated</returns>
        public bool Parse()
        {
            try
            {
                Validated = false;

                //"tcp://0.0.0.0:1"
                if (!string.IsNullOrEmpty(Url))
                {
                    var protoIdx = Url.IndexOf(':') + 3;
                    var portIdx = Url.LastIndexOf(':') + 1;

                    ProtocolDesc = Url[..protoIdx];
                    Port = int.Parse(Url[portIdx..]);
                    Ip = Url.Substring(protoIdx, Url.Length - protoIdx - Url.Length + portIdx - 1);
                    IpPort = $"{Ip}:{Port}";
                    Validated = true;
                }
                Resolve();
            }
            catch (Exception e)
            {
                ValidationErrorString = $"Unable to parse {Url}, must be in the form tcp://IP:port or udp://IP:port. ({e.Message})";
                return false;
            }

            return Validated;
        }

        /// <summary>
        /// Resolves Dns, <see cref="DnsValidated"/> will be set to true
        /// </summary>
        private void Resolve()
        {
            try
            {
                if (Ip == "0.0.0.0" || string.IsNullOrEmpty(Ip))
                {
                    DnsResolutionChanged = false;
                    DnsValidated = true;
                    return;
                }

                if (!Validated)
                {
                    return;
                }

                if (!_performDns)
                {
                    DnsResolutionChanged = false;
                    DnsValidated = false;
                    return;
                }

                var resolvedIpAddress = Dns.GetHostAddresses(IpPort)[0];
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
                Logger.Error(e,$"Unable to resolve host name for `{Url}':");
            }
        }

        public static IoNodeAddress CreateFromEndpoint(string protocol, IPEndPoint endpoint)
        {
            if (endpoint == null)
                throw new ArgumentNullException(nameof(endpoint));

            return new IoNodeAddress($"{protocol}://{endpoint.Address}:{endpoint.Port}", endpoint);
        }

        public override bool Equals(object obj)
        {
            return obj != null && Equals((IoNodeAddress) obj);
        }

        /// <summary>
        /// Determines equality based on IP:Port
        /// </summary>
        /// <param name="other">The comparator</param>
        /// <returns>True if the IPs are the same, false otherwise</returns>
        protected bool Equals(IoNodeAddress other)
        {
            return Port == other.Port && Ip == other.Ip;
        }

        /// <summary>
        /// Hash function
        /// </summary>
        /// <returns>The hash</returns>
        public override int GetHashCode()
        {
            return Key.GetHashCode();
        }

        public IoNodeAddress Copy()
        {
            return (IoNodeAddress)MemberwiseClone();
        }
    }
}
