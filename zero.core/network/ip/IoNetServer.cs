using System;
using System.Collections.Concurrent;
using System.Data;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Transactions;
using Cassandra;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
using Logger = NLog.Logger;

namespace zero.core.network.ip
{
    /// <inheritdoc />
    /// <summary>
    /// A wrap for <see cref="T:zero.core.network.ip.IoSocket" /> to host a server for <see cref="IoNetClient{TJob}"/>s to connect to.
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
        protected IoNetServer(IoNodeAddress listeningAddress, int readAheadBufferSize = 2, int concurrencyLevel = 1) : base($"{nameof(IoNetServer<TJob>)}", concurrencyLevel)
        {
            ListeningAddress = listeningAddress;
            ReadAheadBufferSize = readAheadBufferSize;
            ConcurrencyLevel = concurrencyLevel;

            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// The listening address of this server
        /// </summary>
        public IoNodeAddress ListeningAddress { get; protected set; }

        public new bool CfgAwait => true;

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
        protected IoNetSocket IoListenSocket;

        /// <summary>
        /// A set of currently connecting net clients
        /// </summary>
        private ConcurrentDictionary<string, IoNetClient<TJob>> _connectionAttempts = new();

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
        /// <param name="bootstrapAsync">Bootstrap code</param>
        /// <returns>True on success, false otherwise</returns>
        public virtual ValueTask ListenAsync<T>(Func<T, IoNetClient<TJob>, ValueTask> connectionReceivedAction,
            T nanite = default,
            Func<ValueTask> bootstrapAsync = null)
        {
            if (IoListenSocket != null)
                throw new ConstraintException($"Listener has already been started for `{ListeningAddress}'");
            
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Connect to a host async
        /// </summary>
        /// <param name="remoteAddress">A stub</param>
        /// <param name="ioNetClient">The client to connect to</param>
        /// <param name="timeout"></param>
        /// <returns>The client object managing this socket connection</returns>
        public virtual async ValueTask<IoNetClient<TJob>> ConnectAsync(IoNodeAddress remoteAddress,
            IoNetClient<TJob> ioNetClient = null, int timeout = 0)
        {
            if (!_connectionAttempts.TryAdd(remoteAddress.Key, ioNetClient))
            {
                return null;

                //await Task.Delay(parm_connection_timeout, AsyncTasks.Token).ConfigureAwait(ZC);
                //if (!_connectionAttempts.TryAdd(address.Key, ioNetClient))
                // {
                //     _logger.Warn($"Cancelling existing connection attempt to `{remoteAddress}'");
                //
                //     await _connectionAttempts[remoteAddress.Key].ZeroAsync(this).ConfigureAwait(ZC);
                //     _connectionAttempts.TryRemove(remoteAddress.Key, out _);
                //     return await ConnectAsync(remoteAddress, ioNetClient).ConfigureAwait(ZC);
                // }
            }

            var connected = false;
            try
            {
                connected = await ioNetClient!.ConnectAsync(remoteAddress, timeout).FastPath().ConfigureAwait(CfgAwait);
                if (connected && ioNetClient.IsOperational)
                {
                    //Check things

                    //Ensure ownership
                    //if (!await ZeroAtomicAsync(static async (s,client,_) => (await s.ZeroHiveAsync(client).FastPath().ConfigureAwait(ZC)).success,ioNetClient).FastPath().ConfigureAwait(ZC))
                    //{
                    //    _logger.Trace($"{nameof(ConnectAsync)}: [FAILED], unable to ensure ownership!");
                    //    //REJECT
                    //    connected = false;
                    //}

                    if (!(await ZeroHiveAsync(ioNetClient).FastPath().ConfigureAwait(CfgAwait)).success)
                    {
                        _logger.Trace($"{Description}: {nameof(ConnectAsync)} [FAILED], unable to ensure ownership!");
                        //REJECT
                        connected = false;
                    }

                    _logger.Trace(
                        $"{Description}: {nameof(ConnectAsync)} [SUCCESS], dest = {ioNetClient.IoNetSocket.RemoteNodeAddress}");
                    //ACCEPT
                }
                else // On connection failure after successful connect?
                {
                    _logger.Trace($"{Description}: {nameof(ConnectAsync)} [STALE], {remoteAddress}");
                    //REJECT
                    connected = false;
                }
            }
            catch when (Zeroed()) { }
            catch (Exception e)when (!Zeroed())
            {
                _logger.Error(e, $"{Description}: {nameof(ConnectAsync)} to {remoteAddress.Key} [FAILED]!");
            }
            finally
            {
                if (!connected)
                {
                    await ioNetClient!.ZeroAsync(this).FastPath().ConfigureAwait(CfgAwait);

                    if (!Zeroed())
                        _logger.Error($"{Description}: {nameof(ConnectAsync)} to {remoteAddress.Key} [FAILED]");
                }

                if (!_connectionAttempts.TryRemove(remoteAddress.Key, out _))
                {
                    _logger.Fatal($"Unable find existing connection {remoteAddress.Key},");
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
            _logger = null;
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
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(CfgAwait);
        }

        /// <summary>
        /// Figures out the correct server to use from the url, <see cref="IoTcpServer"/> or <see cref="IoUdpServer"/>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="bufferReadAheadSize"></param>
        /// <param name="concurrencyLevel"></param>
        /// <returns></returns>
        public static IoNetServer<TJob> GetKindFromUrl(IoNodeAddress address, int bufferReadAheadSize, int concurrencyLevel)
        {
            if (address.Protocol() == ProtocolType.Tcp)
                return new IoTcpServer<TJob>(address, bufferReadAheadSize, concurrencyLevel);


            if (address.Protocol() == ProtocolType.Udp)
                return new IoUdpServer<TJob>(address, bufferReadAheadSize, concurrencyLevel);

            return null;
        }
    }
}
