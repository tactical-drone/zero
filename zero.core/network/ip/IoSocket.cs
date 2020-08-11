using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;

namespace zero.core.network.ip
{
    /// <summary>
    /// Abstracts TCP and UDP
    /// </summary>
    public abstract class 
        IoSocket
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
            Socket = new Socket(AddressFamily.InterNetwork, socketType, protocolType);

            _cancellationTokenRegistration = cancellationToken.Register(() => Spinners.Cancel());
        }

        /// <inheritdoc />
        /// <summary>
        /// A copy constructor used by listeners
        /// </summary>
        /// <param name="socket">The listening socket</param>
        /// <param name="listenerAddress">The address listened on</param>
        /// <param name="cancellationToken">Signals all blockers to cancel</param>
        protected IoSocket(Socket socket, IoNodeAddress listenerAddress, CancellationToken cancellationToken)
        {
            _logger = LogManager.GetCurrentClassLogger();
            Socket = socket;
            ListenerAddress = listenerAddress;            

            _cancellationTokenRegistration = cancellationToken.Register(Close);
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The underlying .net socket that is abstracted
        /// </summary>
        protected volatile Socket Socket;

        /// <summary>
        /// Keys this socket
        /// </summary>
        public virtual string Key => RemoteAddress.Key;

        /// <summary>
        /// The original node address this socket is supposed to work with
        /// </summary>
        public IoNodeAddress ListenerAddress { get; protected set; }

        /// <summary>
        /// The original node address this socket is supposed to work with
        /// </summary>
        public IoNodeAddress RemoteAddress { get; protected set; }

        /// <summary>
        /// An indication that this socket is a listening socket
        /// </summary>
        public bool IsListeningSocket { get; protected set; } = false;

        /// <summary>
        /// An indication that this socket is a connecting socket
        /// </summary>
        public bool IsConnectingSocket { get; protected set; } = false;

        /// <summary>
        /// Cancellation sources.
        /// </summary>
        public readonly CancellationTokenSource Spinners = new CancellationTokenSource();

        /// <summary>
        /// Public access to remote address (used for logging)
        /// </summary>
        public string RemoteAddressFallback => Socket?.RemoteAddress()?.ToString() ?? ListenerAddress.IpEndPoint?.Address?.ToString() ?? ListenerAddress.Url;

        /// <summary>
        /// Public access to remote port (used for logging)
        /// </summary>
        public int RemotePort => Socket?.RemotePort() ?? ListenerAddress.IpEndPoint?.Port ?? ListenerAddress.Port;

        /// <summary>
        /// Returns the remote address as a string ip:port
        /// </summary>
        public string RemoteIpAndPort => $"{RemoteAddressFallback}:{RemotePort}";

        /// <summary>
        /// Public access to local address (used for logging)
        /// </summary>
        public string LocalAddress => Socket.LocalAddress().ToString();

        /// <summary>
        /// Public access to local port (used for logging)
        /// </summary>
        public int LocalPort => Socket.LocalPort();

        /// <summary>
        /// Returns the remote address as a string ip:port
        /// </summary>
        public string LocalIpAndPort => $"{LocalAddress}:{LocalPort}";

        /// <summary>
        /// Public access to the underlying socket 
        /// </summary>
        public Socket NativeSocket => Socket;

        /// <summary>
        /// A handle to dispose upstream cancellation hooks
        /// </summary>
        private CancellationTokenRegistration _cancellationTokenRegistration;

        /// <summary>
        /// Returns true if this is a TCP socket
        /// </summary>
        public bool IsTcpSocket => Socket.ProtocolType == ProtocolType.Tcp;


        public bool _closed = false;

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
        /// Listen for TCP or UDP data depending on the URL scheme used. udp://address:port or tcp://address:port
        /// </summary>
        /// <param name="address">Address to listen on</param>
        /// <param name="connectionHandler">The callback that handles a new connection</param>
        /// <returns>True on success, false otherwise</returns>
        public virtual Task<bool> ListenAsync(IoNodeAddress address, Action<IoSocket> connectionHandler)
        {
            //If there was a coding mistake throw
            if (Socket.IsBound)
                throw new InvalidOperationException($"Starting listener failed, socket `{address}' is already bound!");

            if (IsConnectingSocket)
                throw new InvalidOperationException($"This socket was already used to connect to `{ListenerAddress}'. Make a new one!");

            IsListeningSocket = true;

            ListenerAddress = address;

            if (ListenerAddress.Validated)
            {
                try
                {
                    Socket.Bind(ListenerAddress.IpEndPoint);
                    _logger.Debug($"Bound port `{ListenerAddress}'");
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Unable to bind socket at `{ListenerAddress}':");
                    return Task.FromResult(false);
                }

                return Task.FromResult(true);
            }
            else
            {
                _logger.Fatal($"Unable to create listener at: {ListenerAddress?.Url??LocalAddress??"N/A"}. Socket is invalid! ({ListenerAddress.ValidationErrorString})");
                return Task.FromResult(false);
            }
                

        }

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="address">The address to connect to</param>
        /// <returns>True on success, false otherwise</returns>
#pragma warning disable 1998
        public virtual async Task<bool> ConnectAsync(IoNodeAddress address)
#pragma warning restore 1998
        {
            if (Socket.IsBound)
                throw new InvalidOperationException("Cannot connect, socket is already bound!");

            if (IsListeningSocket)
                throw new InvalidOperationException($"This socket was already used to listen at `{ListenerAddress}'. Make a new one!");

            IsConnectingSocket = true;

            RemoteAddress = address;

            return true;
        }

        /// <summary>
        /// Close this socket
        /// </summary>
        public virtual void Close()
        {
            lock (this)
            {
                if (_closed) return;
                _closed = true;
            }

            _logger.Debug($"Closing connection to `{RemoteAddress?.ToString()??LocalIpAndPort}'");

            //This has to be at the top or we might recurse
            _cancellationTokenRegistration.Dispose();

            //Cancel everything that is running
            Spinners.Cancel();

            //Signal to users that we are disconnecting
            OnDisconnected();

            //Close the socket
            //Socket.Shutdown(SocketShutdown.Both);
            Socket?.Close();
            Socket?.Dispose();
            Socket = null;
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
        /// <param name="buffer">The array if bytes to send</param>
        /// <param name="offset">Start at offset</param>
        /// <param name="length">The number of bytes to send</param>
        /// <param name="endPoint">endpoint when required by the socket</param>
        /// <returns></returns>
        public abstract Task<int> SendAsync(byte[] buffer, int offset, int length, EndPoint endPoint = null);

        /// <summary>
        /// Reads a message from the socket
        /// </summary>
        /// <param name="buffer">The buffer to read into</param>
        /// <param name="offset">The offset into the buffer</param>
        /// <param name="length">The maximum bytes to read into the buffer</param>        
        /// <returns>The amounts of bytes read</returns>
        public abstract Task<int> ReadAsync(byte[] buffer, int offset, int length);

        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        public abstract bool IsConnected();


        /// <summary>
        /// Extra data made available to specific uses
        /// </summary>
        /// <returns>Some data</returns>
        public abstract object ExtraData();
    }
}
