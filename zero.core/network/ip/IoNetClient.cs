using System;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.core;
using zero.core.misc;
using zero.core.models;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.network.ip
{
    /// <summary>
    /// Used by clients to hold the <see cref="IoNetSocket"/> of a connection. Used by <see cref="IoNetServer{TJob}.ConnectAsync"/> and <see cref="IoNetServer{TJob}.ListenAsync"/>
    /// to wrap a connection.
    ///
    /// Currently two flavors exist: <see cref="IoTcpClient{TJob}"/> & <see cref="IoUdpClient{TJob}"/>
    ///
    /// Generally:
    /// 
    /// Wraps a <see cref="IoNetSocket"/> into a <see cref="IoSource{TJob}"/> so that it can be used by
    /// <see cref="IoZero{TJob}"/> to produce <see cref="IoJob{TJob}"/>s that are eventually terminated in
    /// <see cref="IoSink{TJob}"/>s.
    ///
    /// The idea:
    ///
    /// Production that waits on consumer back pressure:
    /// <see cref="IIoZero.BlockOnReplicateAsync"/> -> <see cref="IoZero{TJob}.ProduceAsync"/> -> <see cref="IIoSource.ProduceAsync{T}"/> -> <see cref="IIoJob.ProduceAsync"/>
    ///
    /// Consumption that waits on producer pressure:
    /// <see cref="IIoZero.BlockOnReplicateAsync"/> -> <see cref="IoZero{TJob}.ConsumeAsync"/> -> <see cref="IoSink{TJob}.ConsumeAsync"/>
    ///
    /// A Networked Node producer/consumer implementation's base blueprint:
    /// <see cref="IoNode{TJob}.ConnectAsync"/> -> <see cref="IoNeighbor{TJob}.ConsumeAsync"/> -> <see cref="IoMessage{TJob}.ConsumeAsync"/>
    /// 
    /// </summary>
    public abstract class IoNetClient<TJob> : IoSource<TJob>
    where TJob : IIoJob

    {
        /// <summary>
        /// Constructor for incoming connections used by the listener
        /// </summary>
        /// <param name="netSocket">The new socket that comes from the listener</param>
        /// <param name="prefetchSize">The amount of socket reads the upstream is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        protected IoNetClient(string description, IoNetSocket netSocket, int prefetchSize, int concurrencyLevel) : base(description, prefetchSize, concurrencyLevel)
        {
            IoNetSocket = netSocket;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Constructor for connecting
        /// </summary>
        /// <param name="prefetchSize">The amount of socket reads the upstream is allowed to lead the consumer</param>
        /// <param name="concurrencyLevel">Concurrency level</param>
        protected IoNetClient(string description, int prefetchSize, int concurrencyLevel) : base(description, prefetchSize, concurrencyLevel)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        private string _key;
        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>
        /// The unique key of this instance
        /// </returns>
        public override string Key
        {
            get
            {
                if (_key != null)
                    return _key;
                return _key = IoNetSocket?.Key;
            }
        }

        /// <summary>
        /// A description of this client. Currently the remote address
        /// </summary>
        public override string Description => IoNetSocket?.Description??"N/A";
        

        /// <summary>
        /// Abstracted dotnet udp and tcp socket
        /// </summary>
        public IoNetSocket IoNetSocket { get; protected set; }
        
        /// <summary>
        /// Transmit timeout in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_tx_timeout = 3000;

        /// <summary>
        /// Receive timeout in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_rx_timeout = 3000;


        /// <summary>
        /// ZeroAsync unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
            IoNetSocket = null;
#endif
        }

        /// <summary>
        /// ZeroAsync managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
            IoNetSocket.Zero(this);
        }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>True if succeeded, false otherwise</returns>
        public virtual async ValueTask<bool> ConnectAsync(IoNodeAddress remoteAddress, int timeout)
        {
            //fail fast
            if (Zeroed())
                return false;

            var connected = await IoNetSocket.ConnectAsync(remoteAddress, timeout).FastPath().ConfigureAwait(Zc);

            if (connected)
                _logger.Trace($"Connecting to `{remoteAddress}', {Description}");
            else
                _logger.Error($"Failed connecting to `{remoteAddress}', {Description} [FAILED]");

            return connected;
        }

        /// <summary>
        /// Execute the a tcp client function, detect TCP connection drop
        /// </summary>
        /// <param name="callback">The tcp client functions</param>
        /// <param name="jobClosure"></param>
        /// <param name="barrier"></param>
        /// <param name="nanite"></param>
        /// <returns>True on success, false otherwise</returns>
        public override async ValueTask<bool> ProduceAsync<T>(
            Func<IIoNanite, Func<IIoJob, T, ValueTask<bool>>, T, IIoJob, ValueTask<bool>> callback,
            IIoJob jobClosure = null,
            Func<IIoJob, T, ValueTask<bool>> barrier = null,
            T nanite = default)
        {
            try
            {
                return await callback(IoNetSocket, barrier, nanite, jobClosure).FastPath().ConfigureAwait(Zc);
            }
            catch (TimeoutException)
            {
                return false;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (Exception e)
            {
                if(!Zeroed())
                    _logger.Error(e,$"Source `{Description}' callback failed:");
                return false;
            }
        }

        /// <summary>
        /// Backing variable
        /// </summary>
        private volatile bool _operational = false;

        /// <summary>
        /// Rate limit socket health checks
        /// </summary>
        private long _lastSocketHealthCheck = (DateTimeOffset.UtcNow + TimeSpan.FromDays(1)).ToUnixTimeMilliseconds();
        
        /// <summary>
        /// Detects socket drops
        /// </summary>
        /// <returns>True it the connection is up, false otherwise</returns>
        public override bool IsOperational
        {
            get
            {
                try
                {
                    //fail fast
                    if (Zeroed())
                        return false;
                    
                    //check TCP
                    if (IoNetSocket.IsTcpSocket)
                    {
                        //rate limit
                        if (_lastSocketHealthCheck.CurrentMsDelta() < 5000)
                            return _operational && IoNetSocket.NativeSocket.Connected;
                        
                        _lastSocketHealthCheck = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        //TODO more checks?
                        if (!IoNetSocket.IsConnected() 
                            // || 
                            // IoNetSocket.NativeSocket.Poll(-1, SelectMode.SelectError) || 
                            // !IoNetSocket.NativeSocket.Poll(-1, SelectMode.SelectRead) ||
                            // !IoNetSocket.NativeSocket.Poll(-1, SelectMode.SelectWrite)
                            )
                        {
                            if(Uptime.ElapsedMs() > 5)
                                _logger.Error($"DC {IoNetSocket.RemoteNodeAddress} from {IoNetSocket.LocalNodeAddress}, uptime = {TimeSpan.FromSeconds(Uptime.ElapsedMs())}");

                            //Do cleanup
                            return _operational = false;
                        }
                        
                        return _operational = true;
                    }

                    //Check UDP
                    return _operational = (bool) IoNetSocket?.IsConnected();
                }
                catch when (Zeroed()){}
                catch (Exception e) when(!Zeroed())
                {
                    _logger.Trace(e, $"{Description}");
                }
                return _operational = false;
            }            
        }

        /// <summary>
        /// Blacklist a source port
        /// </summary>
        /// <param name="remoteAddressPort"></param>
        public virtual void Blacklist(int remoteAddressPort) {}

        /// <summary>
        /// Whitelist a source port
        /// </summary>
        /// <param name="remoteAddressPort"></param>
        public virtual void WhiteList(int remoteAddressPort) {}
    }
}
