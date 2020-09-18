using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// Wraps a <see cref="TcpClient"/> into a <see cref="IoSource{TJob}"/> that can be used by <see cref="IoZero{TJob}"/>
    /// </summary>
    public abstract class IoNetClient<TJob> : IoSource<TJob>
    where TJob : IIoJob
    
    {
        /// <summary>
        /// Constructor for incoming connections used by the listener
        /// </summary>
        /// <param name="socket">The new socket</param>
        /// <param name="readAheadBufferSize">The amount of socket reads the upstream is allowed to lead the consumer</param>
        protected IoNetClient(IoSocket socket,int readAheadBufferSize) : base(readAheadBufferSize)
        {
            IoSocket = ZeroOnCascade((IoNetSocket)socket, true);

            _logger = LogManager.GetCurrentClassLogger();
            ListeningAddress = socket.ListeningAddress; 
        }

        /// <summary>
        /// Constructor for connecting
        /// </summary>
        /// <param name="listeningAddress">The address associated with this network client</param>
        /// <param name="readAheadBufferSize">The amount of socket reads the upstream is allowed to lead the consumer</param>
        protected IoNetClient(IoNodeAddress listeningAddress, int readAheadBufferSize) : base(readAheadBufferSize)
        {
            ListeningAddress = listeningAddress;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The remote address associated with this client
        /// </summary>
        public IoNodeAddress ListeningAddress;

        /// <summary>
        /// The client remote address
        /// </summary>
        public IoNodeAddress RemoteAddress => IoSocket.RemoteAddress;

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

                _key = IoSocket?.Key;
                return _key;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private string _key;

        /// <summary>
        /// Backing description
        /// </summary>
        private string _description;
        /// <summary>
        /// A description of this client. Currently the remote address
        /// </summary>
        public override string Description
        {
            get 
            { 
                if(_description == null)
                    return _description = $"`net client({IoSocket?.RemoteAddress?.ToString() ?? ListeningAddress.ToString()})'";
                return _description;
            }
        }

        /// <summary>
        /// A description of this client source. Currently the remote address
        /// </summary>
        public override string SourceUri => $"{IoSocket.RemoteAddress}";

        /// <summary>
        /// Abstracted dotnet udp and tcp socket
        /// </summary>
        protected IoNetSocket IoSocket;

        /// <summary>
        /// Access to the underlying socket abstraction
        /// </summary>
        public IoNetSocket Socket => IoSocket;

        /// <summary>
        /// Returns the host address URL in the format tcp://IP:port
        /// </summary>
        public string AddressString => $"{ListeningAddress?.Url}";

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
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            IoSocket = null;
#endif
        }

        /// <summary>
        /// ZeroAsync managed
        /// </summary>
        protected override async Task ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>True if succeeded, false otherwise</returns>
        public virtual async Task<bool> ConnectAsync()
        {            
            var connectAsyncTask = IoSocket.ConnectAsync(ListeningAddress);            

            _logger.Debug($"Connecting to `{ListeningAddress}', {Description}");

            if (await connectAsyncTask)
                return true;
            else
                _logger.Error($"Connecting to `{ListeningAddress}', {Description} [FAILED]");

            return false;
        }

        /// <summary>
        /// Execute the a tcp client function, detect TCP connection drop
        /// </summary>
        /// <param name="callback">The tcp client functions</param>
        /// <param name="barrier"></param>
        /// <param name="zeroClosure"></param>
        /// <param name="jobClosure"></param>
        /// <returns>True on success, false otherwise</returns>

        //public async Task<Task> Execute(Func<IoSocket, Task<Task>> callback)
        public override async ValueTask<bool> ProduceAsync(
            Func<IIoSourceBase, Func<IIoJob, IIoZero, ValueTask<bool>>, IIoZero, IIoJob, Task<bool>> callback,
            Func<IIoJob, IIoZero, ValueTask<bool>> barrier = null, IIoZero zeroClosure = null, IIoJob jobClosure = null)
        {
            //Is the TCP connection up?
            if (!IsOperational)
            {
                return false;
            }

            try
            {
                return await callback(IoSocket, barrier, zeroClosure, jobClosure).ConfigureAwait(false);
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
                _logger.Error(e,$"Source `{Description}' callback failed:");
                return false;
            }
        }

        /// <summary>
        /// Detects socket drops //TODO this needs some work or testing
        /// </summary>
        /// <returns>True it the connection is up, false otherwise</returns>
        public override bool IsOperational
        {
            get
            {
                try
                {
                    if (Zeroed())
                        return false;

                    if (IoSocket == null)
                        return false;

                    if (IoSocket?.NativeSocket != null && IoSocket.IsTcpSocket)
                    {
                        //var selectError = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectError)?"FAILED":"OK";
                        //var selectRead = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectRead)? "OK" : "FAILED";//TODO what is this?
                        //var selectWrite = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectWrite)? "OK" : "FAILED";

                        //TODO more checks?
                        if (!IoSocket.IsConnected() /*|| selectError=="FAILED" || selectRead == "FAILED" || selectWrite == "FAILED" */)
                        {
                            //_logger.Warn($"`{Address}' is in a faulted state, connected={_ioNetClient.Client.Connected}, {SelectMode.SelectError}={selectError}, {SelectMode.SelectRead}={selectRead}, {SelectMode.SelectWrite}={selectWrite}");
                            _logger.Warn($"DC `{ListeningAddress}' from {IoSocket.LocalIpAndPort}");

                            //Do cleanup
                            return false;
                        }

                        return true;
                    }
                    else
                    {
                        return IoSocket?.IsConnected() ?? false;
                    }
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"The connection to `{Description}' has been closed:");
                    return false;
                }
            }            
        }        
    }
}
