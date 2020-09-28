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
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <inheritdoc />
    /// <summary>
    /// A wrap for <see cref="T:zero.core.network.ip.IoSocket" /> to make it host a server
    /// </summary>
    public abstract class IoNetServer<TJob> : IoNanoprobe
    where TJob : IIoJob

    {
        /// <inheritdoc />
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="listeningAddress">The listening address</param>
        /// <param name="readAheadBufferSize">Nr of reads the producer can lead the consumer</param>
        /// <param name="concurrencyLevel">Max Nr of expected concurrent consumers</param>
        protected IoNetServer(IoNodeAddress listeningAddress, int readAheadBufferSize = 1, int concurrencyLevel = 1)
        {
            ListeningAddress = listeningAddress;
            ReadAheadBufferSize = readAheadBufferSize;
            ConcurrencyLevel = concurrencyLevel;

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


        private string _description;
        /// <summary>
        /// A description
        /// </summary>
        public override string Description
        {
            get
            {
                if(_description == null) 
                    return _description = IoListenSocket?.ToString() ?? ListeningAddress?.ToString();
                return _description;
            }
        }

        /// <summary>
        /// The <see cref="TcpListener"/> instance that is wrapped
        /// </summary>
        protected IoSocket IoListenSocket;

        /// <summary>
        /// A set of currently connecting net clients
        /// </summary>
        private ConcurrentDictionary<string, IoNetClient<TJob>> _connectionAttempts = new ConcurrentDictionary<string, IoNetClient<TJob>>();

        /// <summary>
        /// The amount of socket reads the producer is allowed to lead the consumer
        /// </summary>
        protected readonly int ReadAheadBufferSize;
        
        /// <summary>
        /// The number of concurrent consumers
        /// </summary>
        protected readonly int ConcurrencyLevel;
        
        /// <summary>
        /// Connection timeout
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_connection_timeout = 10000;

        /// <summary>
        /// Listens for new connections
        /// </summary>
        /// <param name="connectionReceivedAction">Action to execute when an incoming connection was made</param>
        /// <param name="readAheadBufferSize">TCP read ahead</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        /// <param name="bootstrapAsync">Bootstrap code</param>
        /// <returns>True on success, false otherwise</returns>
        public virtual Task ListenAsync(Func<IoNetClient<TJob>,Task> connectionReceivedAction, Func<Task> bootstrapAsync = null)
        {
            if (IoListenSocket != null)
                throw new ConstraintException($"Listener has already been started for `{ListeningAddress}'");
            
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
            if (ioNetClient == null)
                return null;

            if (!_connectionAttempts.TryAdd(address.Key, ioNetClient))
            {
                //await Task.Delay(parm_connection_timeout, AsyncTasks.Token).ConfigureAwait(false);
                //if (!_connectionAttempts.TryAdd(address.Key, ioNetClient))
                {
                    _logger.Warn($"Cancelling existing connection attempt to `{address}'");

                    await _connectionAttempts[address.Key].ZeroAsync(this).ConfigureAwait(false);
                    _connectionAttempts.TryRemove(address.Key, out _);
                    return await ConnectAsync(address, ioNetClient).ConfigureAwait(false);
                }
            }

            var connected = false;
            try
            {
                connected = await ioNetClient.ConnectAsync().ConfigureAwait(false);
                if (connected)
                {
                    //Check things
                    if (ioNetClient.IsOperational)
                    {
                        //Ensure ownership
                        if (!await ZeroEnsureAsync(s => Task.FromResult(s.ZeroOnCascade(ioNetClient.Nanoprobe).success)).ConfigureAwait(false))
                        {
                            _logger.Debug($"{nameof(ConnectAsync)}: [FAILED], unable to ensure ownership!");
                            //REJECT
                            connected = false;
                        }

                        _logger.Debug($"{nameof(ConnectAsync)}: [SUCCESS], dest = {ioNetClient.ListeningAddress}");
                    }
                    else // On connection failure after successful connect?
                    {
                        _logger.Debug($"{nameof(ConnectAsync)}: [STALE], {ioNetClient.ListeningAddress}");
                        //REJECT
                        connected = false;
                    }

                    //ACCEPT
                }
            }
            catch (TaskCanceledException e) {_logger.Trace(e, Description);}
            catch (NullReferenceException e) { _logger.Trace(e, Description); }
            catch (ObjectDisposedException e) { _logger.Trace(e, Description); }
            catch (OperationCanceledException e) { _logger.Trace(e, Description); }
            finally
            {
                if (!connected)
                {
                    await ioNetClient.ZeroAsync(this).ConfigureAwait(false);

                    if (!Zeroed())
                        _logger.Error($"{nameof(ConnectAsync)}: [FAILED], dest = {ioNetClient.ListeningAddress}");

                    ioNetClient = null;

                    if (!_connectionAttempts.TryRemove(address.Key, out _))
                    {
                        _logger.Fatal($"Unable find existing connection {address.Key},");
                    }
                }
            }

            return ioNetClient;
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            ListeningAddress = null;
            IoListenSocket = null;
            _connectionAttempts = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            _connectionAttempts.Clear();
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }


        /// <summary>
        /// Figures out the correct server to use from the url, <see cref="IoTcpServer"/> or <see cref="IoUdpServer"/>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="bufferReadAheadSize"></param>
        /// <param name="concurrenctyLevel"></param>
        /// <returns></returns>
        public static IoNetServer<TJob> GetKindFromUrl(IoNodeAddress address, int bufferReadAheadSize, int concurrenctyLevel)
        {
            if (address.Protocol() == ProtocolType.Tcp)
                return new IoTcpServer<TJob>(address, bufferReadAheadSize, concurrenctyLevel);


            if (address.Protocol() == ProtocolType.Udp)
                return new IoUdpServer<TJob>(address, bufferReadAheadSize, concurrenctyLevel);

            return null;
        }
    }
}
