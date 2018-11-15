using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using zero.core.core;
using Microsoft.Extensions.Primitives;
using NLog;
using zero.core.conf;

namespace zero.core.network.ip
{
    /// <summary>
    /// A wrap for <see cref="IoSocket"/> to make it host a server
    /// </summary>
    public class IoNetServer:IoConfigurable
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ip">The host ip to listen on</param>
        /// <param name="port">The host port to listen on</param>
        /// <param name="cancellationToken">Kill signal</param>
        public IoNetServer(string ip, int port, CancellationToken cancellationToken)
        {
            _ip = ip;
            _port = port;
            _logger = LogManager.GetCurrentClassLogger();

            _spinners = new CancellationTokenSource();
            cancellationToken.Register(_spinners.Cancel);

        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Cancel all listener tasks
        /// </summary>
        private readonly CancellationTokenSource _spinners;

        /// <summary>
        /// The host ip
        /// </summary>
        private readonly string _ip;

        /// <summary>
        /// The host port
        /// </summary>
        private readonly int _port;

        /// <summary>
        /// The <see cref="TcpListener"/> instance that is wrapped
        /// </summary>
        private IoSocket _listener;

        /// <summary>
        /// The amount of socket reads the producer is allowed to lead the consumer
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_tcp_read_ahead = 5;

        /// <summary>
        /// The Address format in IP:port
        /// </summary>
        public string Address => $"{_ip}:{_port}";


        /// <summary>
        /// A reference to the <see cref="TcpListener"/>
        /// </summary>
        /// public TcpListener TcpListener => _listener;
        /// <summary>
        /// StartAsync a listener
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute on connection</param>
        public async Task<IoNetServer> StartListenerAsync(Action<IoNetClient> connectionReceivedAction)
        {
            if(_listener != null)
                throw new ConstraintException($"tcpLister has already been started for {Address}");

            //async
            //return await Task.Run(async () =>
            {
                _listener = IoSocket.GetKindFromUrl(_ip, _spinners.Token);
                
                await _listener.ListenAsync(IoSocket.StripIp(_ip), _port, socket =>
                {
                    if (socket.NativeSocket.ProtocolType == ProtocolType.Tcp)
                        _logger.Debug($"Got a connection request from `{socket.RemoteAddress}:{socket.RemotePort}'");

                    _logger.Info(socket.NativeSocket.ProtocolType == ProtocolType.Tcp
                        ? $"`{socket.Protocol}{socket.RemoteAddress}:{socket.RemotePort}' connected!"
                        : $"`{socket.Protocol}{socket.LocalAddress}:{socket.LocalPort}' ready for UDP messages");

                    try
                    {
                        //Execute handler
                        connectionReceivedAction?.Invoke(new IoNetClient(socket, parm_tcp_read_ahead));
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, $"Connection received handler returned with errors:");
                        socket.Close();
                    }
                });

                _logger.Debug($"Listener loop `{Address}' exited");
                return this;
            }//, _spinners.Token);
        }

        /// <summary>
        /// Connect to a host async
        /// </summary>
        /// <param name="hostname">The host ip</param>
        /// <param name="port">The host port</param>
        /// <returns>The tcp client wrapper</returns>
        public async Task<IoNetClient> ConnectAsync(string hostname, int port)
        {
            var remoteClientTask = new IoNetClient(hostname, port, parm_tcp_read_ahead);

            //CONNECT
            await remoteClientTask.ConnectAsync().ContinueWith(connectAsync =>
            {
                switch (connectAsync.Status)
                {
                    case TaskStatus.Canceled:
                        _logger.Warn($"Connecting to `{hostname}:{port}' was cancelled");
                        remoteClientTask.Close();
                        break;
                    case TaskStatus.Faulted:
                        _logger.Error(connectAsync.Exception.InnerException??connectAsync.Exception, $"Connecting to `{Address}' was faulted:");
                        remoteClientTask.Close();
                        break;
                    case TaskStatus.RanToCompletion:
                        //On connect success
                        if (remoteClientTask.IsSocketConnected())
                        {
                            _logger.Info($"Connection established `{hostname}:{port}'");
                            break;
                        }
                        else// On connect failure
                        {
                            _logger.Warn($"Unable to connect to `{hostname}:{port}'");
                            remoteClientTask.Close();
                            break;
                        }

                    case TaskStatus.Created:                        
                    case TaskStatus.Running:
                    case TaskStatus.WaitingForActivation:
                    case TaskStatus.WaitingForChildrenToComplete:
                    case TaskStatus.WaitingToRun:
                        throw new InvalidAsynchronousStateException(
                            $"Connecting to `{Address}', state = {connectAsync.Status}");
                }                
            },_spinners.Token);
            return remoteClientTask;
        }
    }
}
