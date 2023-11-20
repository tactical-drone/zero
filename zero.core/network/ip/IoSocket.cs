using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <summary>
    /// Abstracts TCP and UDP
    /// </summary>
    public abstract class IoSocket : IoNanoprobe
    {
        /// <inheritdoc />
        /// <summary>
        /// Constructor, used to create listeners
        /// </summary>
        /// <param name="socketType">The socket type</param>
        /// <param name="protocolType">The protocol type, <see cref="F:System.Net.Sockets.ProtocolType.Tcp" /> or <see cref="F:System.Net.Sockets.ProtocolType.Udp" /></param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        protected IoSocket(SocketType socketType, ProtocolType protocolType, int concurrencyLevel) : base($"{nameof(IoSocket)}", concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            NativeSocket = new Socket(AddressFamily.InterNetwork, socketType, ProtocolType = protocolType);
            Kind = Connection.Egress;
        }

        /// <inheritdoc />
        /// <summary>
        /// Create socket
        /// </summary>
        /// <param name="nativeSocket">The socket to proxy to</param>
        /// <param name="concurrencyLevel">The hub concurrency level</param>
        /// <param name="kind">Incoming or outgoing</param>
        /// <param name="remoteEndPoint">The remote endpoint of this connection in the case of a UDP. TCP unused.</param>
        protected IoSocket(Socket nativeSocket, int concurrencyLevel, Connection kind, EndPoint remoteEndPoint = null) : base($"{nameof(IoSocket)}", concurrencyLevel)
        {
            NativeSocket = nativeSocket ?? throw new ArgumentNullException($"{nameof(nativeSocket)}");

            _logger = LogManager.GetCurrentClassLogger();

            try
            {
                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint(NativeSocket.ProtocolType.ToString().ToLower(),
                    (IPEndPoint)NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint(NativeSocket.ProtocolType.ToString().ToLower(),
                    (IPEndPoint)(NativeSocket.RemoteEndPoint ?? remoteEndPoint));

                Key = NativeSocket.RemoteEndPoint != null ? RemoteNodeAddress.Key : $"{NativeSocket.ProtocolType.ToString().ToLower()}://{remoteEndPoint}";
            }
            catch (ObjectDisposedException)
            {
                Dispose();
                return;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"{nameof(IoSocket)}: ");
            }

            Kind = kind;
            ProtocolType = NativeSocket.ProtocolType;
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;
        private string _description;
        //Socket description 
        public override string Description
        {
            get
            {
                try
                {
                    if(!Zeroed() || _description == null || Environment.TickCount % 100 < 15)
                        return _description = $"{(Proxy ? "[proxy]" : "")}{Kind} socket({LocalNodeAddress}, {(Kind <= Connection.Listener ? "N/A" : RemoteNodeAddress?.ToString())})";
                    return _description;
                }
                catch
                {
                    return _description = $"{(Proxy ? "[proxy]" : "")}{Kind} socket({LocalNodeAddress}, {(Kind <= Connection.Listener ? "N/A" : RemoteNodeAddress?.ToString())})";
                }
            }
        }

        /// <summary>
        /// The underlying .net socket that is abstracted
        /// </summary>
        public Socket NativeSocket { get; internal set; }

        /// <summary>
        /// If this socket is a (udp) proxy
        /// </summary>
        public bool Proxy { get; protected set; }

        /// <summary>
        /// Keys this socket
        /// </summary>
        public string Key { get; private set; }

        /// <summary>
        /// The local address
        /// </summary>
        public IoNodeAddress LocalNodeAddress { get; protected set; }

        /// <summary>
        /// Remote address
        /// </summary>
        public IoNodeAddress RemoteNodeAddress { get; protected set; }

        /// <summary>
        /// The socket initiative
        /// </summary>
        public Connection Kind { get; private set; } = Connection.Undefined;

        /// <summary>
        /// Last socket error
        /// </summary>
        public SocketError LastError { get; protected set; } = SocketError.NotConnected;

        //Lan Address string
        public string LocalAddress => Kind < Connection.Listener || LocalNodeAddress == null ? "(zero)" : LocalNodeAddress.ToString();

        /// <summary>
        /// remote address string
        /// </summary>
        public string RemoteAddress => Kind <= Connection.Listener || RemoteNodeAddress == null ? "(zero)" : RemoteNodeAddress.ToString();

        /// <summary>
        /// Socket 
        /// </summary>
        public enum Connection
        {
            Undefined,
            Listener,
            Ingress,
            Egress,
        }

        /// <summary>
        /// Ingress connection
        /// </summary>
        public bool IsIngress => Kind == Connection.Ingress;

        /// <summary>
        /// Egress connection
        /// </summary>
        public bool IsEgress => Kind == Connection.Egress;

        public ProtocolType ProtocolType { get; private set; }

        /// <summary>
        /// Returns true if this is a TCP socket
        /// </summary>
        public bool IsTcpSocket => ProtocolType == ProtocolType.Tcp;

        /// <summary>
        /// Returns true if this is a TCP socket
        /// </summary>
        public bool IsUdpSocket => ProtocolType == ProtocolType.Udp;

        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_socket_listen_backlog = 16;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

            try
            {
                if (!Proxy)
                    NativeSocket.Dispose();
            }
            catch
            {
                // ignored
            }

#if SAFE_RELEASE
            _logger = null;
            LocalNodeAddress = null;
            RemoteNodeAddress = null;
            NativeSocket = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            Close();
#if DEBUG
            _logger.Trace($"Closed ({ZeroReason})");
#endif
        }

        /// <summary>
        /// prime for zero
        /// </summary>
        /// <returns>The task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ZeroPrime()
        {
            base.ZeroPrime();
            if (!Proxy)
            {
                try
                {
                    if (NativeSocket != null)
                    {
                        NativeSocket.Shutdown(SocketShutdown.Both);
                        NativeSocket.Disconnect(false);
                    }
                }
                catch when (Zeroed()) { }
                catch (Exception e) when (!Zeroed())
                {
                    _logger.Error(e, $"Socket shutdown returned with errors: {Description}");
                }
            }
        }

        /// <summary>
        /// Listen for TCP or UDP data depending on the URL scheme used. udp://address:port or tcp://address:port
        /// </summary>
        /// <param name="listeningAddress">Address to listen on</param>
        /// <param name="acceptConnectionHandler">The callback that handles a new connection</param>
        /// <param name="context"></param>
        /// <param name="bootFunc"></param>
        /// <returns>True on success, false otherwise</returns>
        public virtual ValueTask BlockOnListenAsync<T,TContext>(IoNodeAddress listeningAddress,
            Func<IoSocket, T, ValueTask> acceptConnectionHandler,
            T context,
            Func<TContext, ValueTask> bootFunc = null, TContext bootData = default)
        {
            //If there was a coding mistake throw
            if (NativeSocket.IsBound)
                throw new InvalidOperationException($"Starting listener failed, socket `{listeningAddress}' is already bound!");

            try
            {
                NativeSocket.Bind(listeningAddress.IpEndPoint);
                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint(listeningAddress.Protocol().ToString().ToLower(),(IPEndPoint) NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.Create($"{listeningAddress.ProtocolDesc}0.0.0.0:709");

                Key = LocalNodeAddress.Key;

                Kind = Connection.Listener;

                _logger.Trace($"Bound port {LocalNodeAddress}: {Description}");
            }
            catch (Exception e) when (Zeroed())
            {
                return new ValueTask(Task.FromException(e));                
            }
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e, $"Unable to bind socket at {listeningAddress}: {Description}");
                return new ValueTask(Task.FromException(e));
            }

            return new ValueTask(Task.CompletedTask);
        }

        /// <summary>
        /// Connect to a remote endpoint
        /// </summary>
        /// <param name="remoteAddress">The address to connect to</param>
        /// <param name="timeout"></param>
        /// <returns>True on success, false otherwise</returns>
#pragma warning disable 1998
        public virtual async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout = 0)
#pragma warning restore 1998
        {
            if (NativeSocket == null)
                throw new ArgumentNullException(nameof(NativeSocket));

            if (NativeSocket.IsBound)
                throw new InvalidOperationException("Cannot connect, socket is already bound!");

            if (IsIngress)
                throw new InvalidOperationException($"This socket was already used to listen at `{LocalNodeAddress}'. Make a new one ore reset this one!");

            Key = remoteAddress.Key;

            return true;
        }

        /// <summary>
        /// Send bytes to remote socket
        /// </summary>
        /// <param name="buffer">The array if bytes to send</param>
        /// <param name="offset">Start at offset</param>
        /// <param name="length">The number of bytes to send</param>
        /// <param name="endPoint">endpoint when required by the socket</param>
        /// <param name="crc">crc</param>
        /// <param name="timeout">Send timeout</param>
        /// <returns></returns>
        public abstract ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length,
            EndPoint endPoint = null, long crc = 0,
            int timeout = 0);

        /// <summary>
        /// Reads a message from the socket
        /// </summary>
        /// <param name="buffer">The buffer to read into</param>
        /// <param name="offset">The offset into the buffer</param>
        /// <param name="length">The maximum bytes to read into the buffer</param>
        /// <param name="remoteEp"></param>
        /// <param name="timeout">Sync read with timeout</param>
        /// <returns>The amounts of bytes read</returns>
        public abstract ValueTask<int> ReceiveAsync(Memory<byte> buffer, int offset, int length, byte[] remoteEp = null,
            int timeout = 0);

        /// <summary>
        /// Connection status
        /// </summary>
        /// <returns>True if the connection is up, false otherwise</returns>
        public abstract bool IsConnected();

        /// <summary>
        /// Closes a socket
        /// </summary>
        /// <returns>A task</returns>
        protected void Close()
        {
            try
            {
                if (Proxy || NativeSocket == null) return;

                if (NativeSocket.IsBound || NativeSocket.Connected)
                {
                    try
                    {
                        NativeSocket.Shutdown(SocketShutdown.Both);
                        NativeSocket.Disconnect(false);
                    }
                    catch
                    {
                        // ignored
                    }
                }

                try
                {
                    NativeSocket.Close();
                    NativeSocket.Dispose();
                }
                catch
                {
                    // ignored
                }
            }
            catch (ObjectDisposedException)
            {
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"Socket shutdown returned with errors: {Description}");
            }
            finally
            {
                if(!Proxy)
                    NativeSocket?.Dispose();
            }
        }

        protected abstract void ConfigureSocket();

        /// <summary>
        /// Resets the socket for re-use
        /// </summary>
        private void ResetSocket()
        {
            Close();
            NativeSocket = new Socket(NativeSocket.AddressFamily, NativeSocket.SocketType, NativeSocket.ProtocolType);
            Kind = Connection.Undefined;
            ConfigureSocket();
        }

        /// <summary>
        /// Attempts to reconnect the socket
        /// </summary>
        /// <returns></returns>
        public ValueTask<bool> ReconnectAsync()
        {
            ResetSocket();
            return ConnectAsync(RemoteNodeAddress);
        }
    }
}
