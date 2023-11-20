using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using zero.core.conf;
using zero.core.misc;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <summary>
    /// Marks the more generic <see cref="IoSocket"/> for use in our abstraction
    /// </summary>
    /// <seealso cref="zero.core.network.ip.IoSocket" />
    /// <seealso cref="IIoSource" />
    public abstract class IoNetSocket : IoSocket
    {
        /// <summary>
        /// TCP and UDP connections
        /// </summary>
        /// <param name="socketType"></param>
        /// <param name="protocolType"></param>
        /// <param name="concurrencyLevel"></param>
        protected IoNetSocket(SocketType socketType, ProtocolType protocolType, int concurrencyLevel) : base(socketType, protocolType, concurrencyLevel)
        {

        }

        /// <summary>
        /// Used by listeners
        /// </summary>
        /// <param name="nativeSocket">The socket to wrap</param>
        /// <param name="kind"></param>
        /// <param name="remoteEndPoint">Optional remote endpoint specification</param>
        /// <param name="concurrencyLevel"></param>
        protected IoNetSocket(Socket nativeSocket, Connection kind, EndPoint remoteEndPoint = null,
            int concurrencyLevel = 1) : base(nativeSocket, concurrencyLevel, kind, remoteEndPoint)
        {
            
        }

        /// <summary>
        /// Whether the last operation was a timed op.
        /// </summary>
        protected int _timedOp;

        /// <summary>
        /// DupChecker on send
        /// </summary>
        protected ConcurrentDictionary<long, long> DupChecker = new();

            /// <summary>
        /// Enable TCP keep alive
        /// </summary>
        [IoParameter]
        private bool parm_enable_tcp_keep_alive = false;

#if NET6_0
        [IoParameter]
        private int parm_enable_tcp_keep_alive_time = 120;
        [IoParameter]
        private int parm_enable_tcp_keep_alive_retry_interval_sec = 2;
        [IoParameter]
        private int parm_enable_tcp_keep_alive_retry_count = 2;
#endif

        /// <summary>
        /// Configure Socket
        /// </summary>
        protected override void ConfigureSocket()
        {
            if (NativeSocket.IsBound || NativeSocket.Connected)
            {
                return;
            }

            //NativeSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.DontLinger, true);

            NativeSocket.DontFragment = true;

            NativeSocket.Blocking = true;

            // Don't allow another socket to bind to this port.
            NativeSocket.ExclusiveAddressUse = true;

            // The socket will linger for 10 seconds after
            // Socket.Close is called.
            NativeSocket.LingerState = new LingerOption(false, 0);

            // Disable the Nagle Algorithm for this tcp socket.
            NativeSocket.NoDelay = true;

            // Set the receive buffer size to MTU times
            NativeSocket.ReceiveBufferSize = 1492 * ZeroConcurrencyLevel * 4;

            // Set the send buffer size to MTU times
            NativeSocket.SendBufferSize = 1492 * ZeroConcurrencyLevel * 4;

            // Set the timeout for synchronous receive methods to
            // 1 second (1000 milliseconds.)
            NativeSocket.ReceiveTimeout = 1000;

            // Set the timeout for synchronous send methods
            // to 1 second (1000 milliseconds.)
            NativeSocket.SendTimeout = 1000;

            // Set the Time To Live (TTL) to 64 router hops.
            NativeSocket.Ttl = 64;

            //var v = new uint[] {1, 1000, 2000}.SelectMany(BitConverter.GetBytes).ToArray();
            //var r = NativeSocket.IOControl(
            //    IOControlCode.KeepAliveValues,
            //    v,
            //    null
            //);

            if(NativeSocket.ProtocolType  == ProtocolType.Tcp && parm_enable_tcp_keep_alive)
            {
                NativeSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
#if NET6_0
                NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, parm_enable_tcp_keep_alive_retry_interval_sec);
                NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, parm_enable_tcp_keep_alive_time);
                NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, parm_enable_tcp_keep_alive_retry_count);
#endif
            }

            //_logger.Trace($"Tcp Socket configured: {Description}:" +
            //              $"  ExclusiveAddressUse {socket.ExclusiveAddressUse}" +
            //              $"  LingerState {socket.LingerState.Enabled}, {socket.LingerState.LingerTime}" +
            //              $"  NoDelay {socket.NoDelay}" +
            //              $"  ReceiveBufferSize {socket.ReceiveBufferSize}" +
            //              $"  ReceiveTimeout {socket.ReceiveTimeout}" +
            //              $"  SendBufferSize {socket.SendBufferSize}" +
            //              $"  SendTimeout {socket.SendTimeout}" +
            //              $"  Ttl {socket.Ttl}" +
            //              $"  IsBound {socket.IsBound}");
        }

        public override async ValueTask BlockOnListenAsync<T, TContext>(IoNodeAddress listeningAddress, Func<IoSocket, T, ValueTask> acceptConnectionHandler, T context,
            Func<TContext, ValueTask> bootFunc = null, TContext bootData = default)
        {
            await PruneDupCheckerAsync().FastPath();
            await base.BlockOnListenAsync(listeningAddress, acceptConnectionHandler, context, bootFunc, bootData);
        }

        private async ValueTask PruneDupCheckerAsync()
        {
            await ZeroAsync(static async @this =>
            {
                while (!@this.Zeroed())
                {
                    await Task.Delay(60000);
                    foreach (var entry in @this.DupChecker)
                    {
                        if (entry.Value.ElapsedMs() > 120000)
                            @this.DupChecker.TryRemove(entry.Key, out _);
                    }
                }
            }, this);
        }
    }
}
