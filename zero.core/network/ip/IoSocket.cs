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
        /// Constructor, used to create local clients
        /// </summary>
        /// <param name="socketType">The socket type</param>
        /// <param name="protocolType">The protocol type, <see cref="F:System.Net.Sockets.ProtocolType.Tcp" /> or <see cref="F:System.Net.Sockets.ProtocolType.Udp" /></param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        protected IoSocket(SocketType socketType, ProtocolType protocolType, int concurrencyLevel) : base($"{nameof(IoSocket)}", concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
            NativeSocket = new Socket(AddressFamily.InterNetwork, socketType, protocolType);
        }

        /// <inheritdoc />
        /// <summary>
        /// Used by (UDP) listeners to create ingress proxy
        /// </summary>
        /// <param name="nativeSocket">The socket to proxy to</param>
        /// <param name="concurrencyLevel">The hub concurrency level</param>
        /// <param name="remoteEndPoint">The remote endpoint of this connection in the case of a UDP. TCP unused.</param>
        protected IoSocket(Socket nativeSocket, int concurrencyLevel, EndPoint remoteEndPoint = null) : base($"{nameof(IoSocket)}", concurrencyLevel)
        {
            NativeSocket = nativeSocket ?? throw new ArgumentNullException($"{nameof(nativeSocket)}");

            _logger = LogManager.GetCurrentClassLogger();

            try
            {
                LocalNodeAddress = IoNodeAddress.CreateFromEndpoint(NativeSocket.ProtocolType.ToString().ToLower(),
                    (IPEndPoint)NativeSocket.LocalEndPoint);
                RemoteNodeAddress = IoNodeAddress.CreateFromEndpoint(NativeSocket.ProtocolType.ToString().ToLower(),
                    (IPEndPoint)(NativeSocket.RemoteEndPoint ?? remoteEndPoint));

                Key = NativeSocket.RemoteEndPoint != null ? RemoteNodeAddress.Key : LocalNodeAddress.Key;
            }
            catch (ObjectDisposedException)
            {
                Task.Factory.StartNew(@this => ((IoSocket)@this).Zero((IoSocket)@this, "RACE"),this);
                return;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"{nameof(IoSocket)}: ");
            }

            Kind = Connection.Ingress;

        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;


        //Socket description 
        public override string Description
        {
            get
            {
                try
                {
                    return
                        $"{(Proxy ? "[proxy]" : "")}{Kind} socket({LocalNodeAddress}({NativeSocket?.LocalEndPoint}), {(Kind <= Connection.Listener ? "N/A" : RemoteNodeAddress?.ToString())}({NativeSocket?.RemoteEndPoint}), bound = {NativeSocket?.IsBound}";
                }
                catch (Exception e)
                {
                    return $"{(Proxy ? "[proxy]" : "")}{Kind} socket({LocalNodeAddress}, ({e.Message}), {(Kind <= Connection.Listener ? "N/A" : RemoteNodeAddress?.ToString())}, bound = {NativeSocket?.IsBound}";
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

        //Local Address string
        public string LocalAddress => Kind < Connection.Listener || LocalNodeAddress == null ? "(zero)" : LocalNodeAddress.ToString();

        /// <summary>
        ///
        /// </summary>
        public IoNodeAddress RemoteNodeAddress { get; protected set; }

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
        /// The socket initiative
        /// </summary>
        public Connection Kind { get; private set; } = Connection.Undefined;

        /// <summary>
        /// Ingress connection
        /// </summary>
        public bool IsIngress => Kind == Connection.Ingress;

        /// <summary>
        /// Egress connection
        /// </summary>
        public bool IsEgress => Kind == Connection.Egress;

        /// <summary>
        /// Returns true if this is a TCP socket
        /// </summary>
        public bool IsTcpSocket => NativeSocket?.ProtocolType == ProtocolType.Tcp;

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
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            Close();
#if DEBUG
            _logger.Trace($"Closed {Description} from {ZeroedFrom}: reason = {ZeroReason}");
#endif
        }

        /// <summary>
        /// prime for zero
        /// </summary>
        /// <returns>The task</returns>
        public override async ValueTask ZeroPrimeAsync()
        {
            await base.ZeroPrimeAsync().FastPath().ConfigureAwait(Zc);
            if (!Proxy && NativeSocket.IsBound && NativeSocket.Connected)
            {
                try
                {
                    if (!Proxy && NativeSocket.IsBound && NativeSocket.Connected)
                    {
                        NativeSocket.Shutdown(SocketShutdown.Both);
                        NativeSocket.Disconnect(true);
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
        /// <param name="bootstrapAsync"></param>
        /// <returns>True on success, false otherwise</returns>
        public virtual ValueTask BlockOnListenAsync<T>(IoNodeAddress listeningAddress,
            Func<IoSocket, T, ValueTask> acceptConnectionHandler,
            T context,
            Func<ValueTask> bootstrapAsync = null)
        {
            //If there was a coding mistake throw
            if (NativeSocket.IsBound)
                throw new InvalidOperationException($"Starting listener failed, socket `{listeningAddress}' is already bound!");

            if (Kind != Connection.Undefined)
                throw new InvalidOperationException($"This socket was already used to connect to `{listeningAddress}'. Make a new one!");

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

            return default;
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

            if (Kind != Connection.Undefined)
                throw new InvalidOperationException($"This socket was already used to listen at `{LocalNodeAddress}'. Make a new one ore reset this one!");

            Key = remoteAddress.Key;

            Kind = Connection.Egress;

            return true;
        }

        /// <summary>
        /// Send bytes to remote socket
        /// </summary>
        /// <param name="buffer">The array if bytes to send</param>
        /// <param name="offset">Start at offset</param>
        /// <param name="length">The number of bytes to send</param>
        /// <param name="endPoint">endpoint when required by the socket</param>
        /// <param name="timeout">Send timeout</param>
        /// <returns></returns>
        public abstract ValueTask<int> SendAsync(ReadOnlyMemory<byte> buffer, int offset, int length, EndPoint endPoint = null, int timeout = 0);

        /// <summary>
        /// Reads a message from the socket
        /// </summary>
        /// <param name="buffer">The buffer to read into</param>
        /// <param name="offset">The offset into the buffer</param>
        /// <param name="length">The maximum bytes to read into the buffer</param>
        /// <param name="remoteEp"></param>
        /// <param name="timeout">Sync read with timeout</param>
        /// <returns>The amounts of bytes read</returns>
        public abstract ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int length, byte[] remoteEp = null,
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
                        NativeSocket.Disconnect(true);
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
        public async ValueTask<bool> ReconnectAsync()
        {
            ResetSocket();
            return await ConnectAsync(RemoteNodeAddress).FastPath().ConfigureAwait(Zc);
        }
    }
}
