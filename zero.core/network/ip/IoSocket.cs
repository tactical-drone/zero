using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.misc;
using NLog;
using zero.core.conf;
using zero.core.models;
using zero.core.patterns.bushes;

namespace zero.core.network.ip
{
    /// <summary>
    /// Abstracts TCP and UDP
    /// </summary>
    public abstract class IoSocket : IoConcurrentProcess
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="socketType">The socket type</param>
        /// <param name="protocolType">The protocol type, <see cref="F:System.Net.Sockets.ProtocolType.Tcp" /> or <see cref="F:System.Net.Sockets.ProtocolType.Udp" /></param>
        /// <param name="cancellationToken">Signals all blockers to cancel</param>
        protected IoSocket(SocketType socketType, ProtocolType protocolType, CancellationToken cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
            RawSocket = new Socket(AddressFamily.InterNetwork, socketType, protocolType);

            _cancellationTokenRegistration = cancellationToken.Register(() => Spinners.Cancel());
        }

        /// <inheritdoc />
        /// <summary>
        /// A copy constructor used by listeners
        /// </summary>
        /// <param name="rawSocket">The listening socket</param>
        /// <param name="address">The address is is listening on</param>
        /// <param name="port">The port it is listening on</param>
        /// <param name="cancellationToken">Signals all blockers to cancel</param>
        protected IoSocket(Socket rawSocket, string address, int port, CancellationToken cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
            RawSocket = rawSocket;
            _localAddress = address;
            _localPort = port;

            _cancellationTokenRegistration = cancellationToken.Register(() => Spinners.Cancel());
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The underlying .net socket that is abstracted
        /// </summary>
        protected Socket RawSocket;

        /// <summary>
        /// A string depicting protocol type url preable
        /// </summary>
        public string Protocol => RawSocket?.SocketType == SocketType.Stream? "tcp://":"udp://";
        
        /// <summary>
        /// The remote address (used for logging)
        /// </summary>
        private string _remoteAddress = "[url not set]";

        /// <summary>
        /// Remote port (used for logging)
        /// </summary>
        private int _remotePort = 0;

        /// <summary>
        /// The local address (used for logging)
        /// </summary>
        private string _localAddress = "[url not set]";

        /// <summary>
        /// The local port (used for logging)
        /// </summary>
        private int _localPort = 0;

        /// <summary>
        /// The remote endpoint used to make UDP more connection orientated
        /// </summary>
        protected IPEndPoint RemoteEndPoint;

        /// <summary>
        /// Public access to remote address (used for logging)
        /// </summary>
        public string RemoteAddress => RawSocket?.RemoteAddress()?.ToString() ?? _remoteAddress;

        /// <summary>
        /// Public access to remote port (used for logging)
        /// </summary>
        public int RemotePort => RawSocket?.RemotePort()?? _remotePort;

        /// <summary>
        /// Public access to local address (used for logging)
        /// </summary>
        public string LocalAddress => RawSocket.LocalAddress().ToString();

        /// <summary>
        /// Public access to local port (used for logging)
        /// </summary>
        public int LocalPort => RawSocket.LocalPort();

        /// <summary>
        /// Public access to the underlying socket 
        /// </summary>
        public Socket NativeSocket => RawSocket;

        /// <summary>
        /// A handle to dispose upstream cancellation hooks
        /// </summary>
        private readonly CancellationTokenRegistration _cancellationTokenRegistration;

        /// <summary>
        /// Used to chain two consecutive receives together so that fragments can be combined correctly. 
        /// </summary>
        protected volatile uint RecvCount = 0;

        /// <summary>
        /// Required because Socket.BeginRecv is not reentrant
        /// </summary>
        protected SemaphoreSlim RecvSemaphoreSlim = new SemaphoreSlim(1);

        /// <summary>
        /// Required because Socket.BeginSend is not reentrant
        /// </summary>
        protected SemaphoreSlim SendSemaphoreSlim = new SemaphoreSlim(1);

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_socket_listen_backlog = 20;


        /// <summary>
        /// Parses the url string and returns either a TCP or UDP <see cref="IoSocket"/>
        /// </summary>
        /// <param name="url">The url</param>
        /// <param name="spinner">A hook to cancel blockers</param>
        /// <returns></returns>
        public static IoSocket GetKindFromUrl(string url, CancellationToken spinner)
        {
            if (url.Contains("tcp://"))
                return new IoTcpSocket(spinner);
            else if (url.Contains("udp://"))
                return new IoUdpSocket(spinner);
            else
            {
                throw new UriFormatException($"URI string `{url}' must be in the format tcp://ip:port or udp://ip");
            }
        }

        /// <summary>
        /// Strips the IP from a URL string
        /// </summary>
        /// <param name="url">The url to be stripped</param>
        /// <returns>The ip contained in the url</returns>
        public static string StripIp(string url)
        {
            return url.Replace("tcp://", "").Replace("udp://", "").Split(":")[0];
        }

        /// <summary>
        /// Listen for TCP or UDP data depending on the URL scheme used. udp://address:port or tcp://address:port
        /// </summary>
        /// <param name="address">The listen address</param>
        /// <param name="port">The listen port</param>
        /// <param name="callback">The callback that accesses a newly created socket</param>
        public virtual Task ListenAsync(string address, int port, Action<IoSocket> callback)
        {
            if (RawSocket.IsBound)
                throw new InvalidOperationException("Cannot connect, socket is already bound!");

            RemoteEndPoint = new IPEndPoint(IPAddress.Parse(address), port);
                    
            RawSocket.Bind(RemoteEndPoint);            

            _localAddress = address;
            _localPort = port;

            return Task.CompletedTask;
        }

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="address">The remote ip address to connect to</param>
        /// <param name="port">The remote port</param>
        /// <returns></returns>
        public async Task ConnectAsync(string address, int port)
        {            
            if(RawSocket.IsBound)
                throw new InvalidOperationException("Cannot connect, socket is already bound!");

            _remoteAddress = address;
            _remotePort = port;

            //tcp
            if (RawSocket.ProtocolType == ProtocolType.Tcp)
            {
                await RawSocket.ConnectAsync(address, port).ContinueWith(r =>
                {
                    switch (r.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(r.Exception, $"Connecting to `{Protocol}{RemoteAddress}:{RemotePort}' failed.");
                            break;
                        case TaskStatus.RanToCompletion:
                            _logger.Info($"Connected to `{Protocol}{RemoteAddress}:{RemotePort}");

                            break;
                    }
                }, Spinners.Token);
            }
            else //udp
            {
                //set UDP connection orientated things
                RemoteEndPoint = new IPEndPoint(Dns.GetHostAddresses(_remoteAddress)[0], _remotePort);
            }
        }

        /// <summary>
        /// Close this socket
        /// </summary>
        public void Close()
        {
            _cancellationTokenRegistration.Dispose();
            Spinners.Cancel();
            OnDisconnected();
            RawSocket?.Close();
            RawSocket?.Dispose();
            RawSocket = null;
        }

        /// <summary>
        /// Signals remote endpoint disconnections
        /// </summary>
        public event EventHandler Disconnected;

        /// <summary>
        /// Disconnect event hander boilerplate
        /// </summary>
        protected virtual void OnDisconnected()
        {
            Disconnected?.Invoke(this, new EventArgs());
        }

        /// <summary>
        /// Send bytes to remote socket
        /// </summary>
        /// <param name="getBytes">The array if bytes to send</param>
        /// <param name="offset">Start at offset</param>
        /// <param name="length">The number of bytes to send</param>
        /// <returns></returns>
        public abstract Task<int> SendAsync(byte[] getBytes, int offset, int length);
      
        /// <summary>
        /// Reads a message from the socket
        /// </summary>
        /// <param name="message">The buffer for this receive </param>
        /// <returns>The amounts of bytes read</returns>
        public abstract Task<int> ReadAsync(IoMessage<IoNetClient> message);
        
    }
}
