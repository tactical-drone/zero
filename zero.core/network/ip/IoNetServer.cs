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
        /// <param name="listeningAddress">The listening address</param>
        /// <param name="cancellationToken">Kill signal</param>
        public IoNetServer(IoNodeAddress listeningAddress, CancellationToken cancellationToken)
        {
            _listeningAddress = listeningAddress;

            _logger = LogManager.GetCurrentClassLogger();

            _spinners = new CancellationTokenSource();
            cancellationToken.Register(_spinners.Cancel);

        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The listening address of this server
        /// </summary>
        private readonly IoNodeAddress _listeningAddress;

        /// <summary>
        /// Cancel all listener tasks
        /// </summary>
        private readonly CancellationTokenSource _spinners;

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
        public string Address => $"{_listeningAddress.Url}:{_listeningAddress.Port}";


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
                _listener = IoSocket.GetKindFromUrl(_listeningAddress.Url, _spinners.Token);
                
                await _listener.ListenAsync(IoSocket.StripIp(_listeningAddress.Url), _listeningAddress.Port, socket =>
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
        /// <param name="address">The remote node address</param>
        /// <returns>The tcp client wrapper</returns>
        public async Task<IoNetClient> ConnectAsync(IoNodeAddress address)
        {
            var remoteClientTask = new IoNetClient(address.Url, address.Port, parm_tcp_read_ahead);

            //CONNECT
            await remoteClientTask.ConnectAsync().ContinueWith(connectAsync =>
            {
                switch (connectAsync.Status)
                {
                    case TaskStatus.Canceled:
                        _logger.Warn($"Connecting to `{address.Url}:{address.Port}' was cancelled");
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
                            _logger.Info($"Connection established `{address.Url}:{address.Port}'");
                            break;
                        }
                        else// On connect failure
                        {
                            _logger.Warn($"Unable to connect to `{address.Url}:{address.Port}'");
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
