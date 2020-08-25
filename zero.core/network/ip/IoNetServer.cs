using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Protocol;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <inheritdoc />
    /// <summary>
    /// A wrap for <see cref="T:zero.core.network.ip.IoSocket" /> to make it host a server
    /// </summary>
    public abstract class IoNetServer<TJob> : IoConfigurable
    where TJob : IIoJob

    {
        /// <inheritdoc />
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="listeningAddress">The listening address</param>
        protected IoNetServer(IoNodeAddress listeningAddress)
        {
            ListeningAddress = listeningAddress;

            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The listening address of this server
        /// </summary>
        public IoNodeAddress ListeningAddress { get; protected set; }

        /// <summary>
        /// A description
        /// </summary>
        public string Description => IoListenSocket?.ToString() ?? ListeningAddress.ToString();

        /// <summary>
        /// The <see cref="TcpListener"/> instance that is wrapped
        /// </summary>
        protected IoSocket IoListenSocket;

        /// <summary>
        /// A set of currently connecting net clients
        /// </summary>
        private readonly ConcurrentDictionary<string, IoNetClient<TJob>> _connectionAttempts = new ConcurrentDictionary<string, IoNetClient<TJob>>();

        /// <summary>
        /// The amount of socket reads the producer is allowed to lead the consumer
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_read_ahead = 1;

        /// <summary>
        /// Listens for new connections
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute when an incoming connection was made</param>
        /// <param name="readAhead">TCP read ahead</param>
        /// <returns>True on success, false otherwise</returns>
        public virtual Task ListenAsync(Action<IoNetClient<TJob>> connectionReceivedAction, int readAhead)
        {
            if (IoListenSocket != null)
                throw new ConstraintException($"Listener has already been started for `{ListeningAddress}'");

            parm_read_ahead = readAhead;
            return Task.CompletedTask;
        }

        /// <summary>
        /// Connect to a host async
        /// </summary>
        /// <param name="address">A stub</param>
        /// <param name="ioNetClient">The client to connect to</param>
        /// <returns>The client object managing this socket connection</returns>
        public virtual async Task<IoNetClient<TJob>> ConnectAsync(IoNodeAddress address, IoNetClient<TJob> ioNetClient = null)
        {
            if (!_connectionAttempts.TryAdd(address.Key, ioNetClient))
            {
                _logger.Warn($"Cancelling existing connection attemp to `{address}'");
#pragma warning disable 4014
                _connectionAttempts[address.Key].Zero();
#pragma warning restore 4014

                _connectionAttempts.TryRemove(address.Key, out _);
                await Task.Delay(500).ConfigureAwait(false);
                return await ConnectAsync(address, ioNetClient).ConfigureAwait(false);
            }

            try
            {
                var connectTask = ioNetClient?.ConnectAsync().ContinueWith(t =>
                {
                    switch (t.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            _logger.Error(t.Exception, $"Failed to connect to `{ioNetClient.AddressString}':");
                            ioNetClient.Zero();
                            break;
                        case TaskStatus.RanToCompletion:
                            if (ioNetClient.IsOperational)
                            {
                                _logger.Info($"Connection established to `{ioNetClient.AddressString}'");
                                return true;
                            }
                            else // On connect failure
                            {
                                _logger.Warn($"Unable to connect to `{ioNetClient.AddressString}'");
                                ioNetClient.Zero();
                                return false;
                            }
                    }

                    return false;
                }, AsyncTasks.Token);

                //ioNetClient will never be null, the null in the parameter is needed for the interface contract
                if (ioNetClient != null && await connectTask.ConfigureAwait(false))
                {
                    ZeroOnCascade(ioNetClient);
                    return ioNetClient;
                }
            }
            catch (ObjectDisposedException){}
            catch (OperationCanceledException) {}
            finally
            {
                if (!_connectionAttempts.TryRemove(address.Key, out _))
                {
                    _logger.Fatal($"Expected key `{address.Key}'");
                }

                if (_connectionAttempts.Count > 0)
                {
                    _logger.Warn($"Empty connection attempts expected!");
                    _connectionAttempts.Clear();
                }
            }

            return null;
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            base.ZeroManaged();
            _logger.Debug($"{ToString()}: Zeroed {Description}");
        }


        /// <summary>
        /// Figures out the correct server to use from the url, <see cref="IoTcpServer"/> or <see cref="IoUdpServer"/>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="bufferReadAheadSize"></param>
        /// <returns></returns>
        public static IoNetServer<TJob> GetKindFromUrl(IoNodeAddress address, int bufferReadAheadSize)
        {
            if (address.Protocol() == ProtocolType.Tcp)
                return new IoTcpServer<TJob>(address, bufferReadAheadSize);


            if (address.Protocol() == ProtocolType.Udp)
                return new IoUdpServer<TJob>(address);

            return null;
        }
    }
}
