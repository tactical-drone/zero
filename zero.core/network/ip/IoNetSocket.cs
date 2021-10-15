﻿using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
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
        /// <param name="remoteEndPoint">Optional remote endpoint specification</param>
        protected IoNetSocket(Socket nativeSocket, EndPoint remoteEndPoint = null) : base(nativeSocket, 1,remoteEndPoint)
        {

        }

        /// <summary>
        /// Configures the socket
        /// </summary>
        protected virtual void Configure()
        {
            if (NativeSocket.IsBound || NativeSocket.Connected)
            {
                return;
            }

            // Don't allow another socket to bind to this port.
            NativeSocket.ExclusiveAddressUse = true;

            // The socket will linger for 10 seconds after
            // Socket.Close is called.
            NativeSocket.LingerState = new LingerOption(false, 0);

            // Disable the Nagle Algorithm for this tcp socket.
            NativeSocket.NoDelay = false;

            // Set the receive buffer size to 32k
            NativeSocket.ReceiveBufferSize = 8192 * 4;

            // Set the send buffer size to 32k.
            NativeSocket.SendBufferSize = 8192 * 4;

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

            NativeSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 2);
            NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);
            NativeSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 1);

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
    }
}
