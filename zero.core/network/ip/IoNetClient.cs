using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.patterns.bushes;

namespace zero.core.network.ip
{
    /// <summary>
    /// Wraps a <see cref="TcpClient"/> into a <see cref="IoConcurrentProcess"/> that can be used by <see cref="IoProducerConsumer{TJob,TSource}"/>
    /// </summary>
    public class IoNetClient: IoConcurrentProcess
    {
        /// <summary>
        /// Conctructor for listening
        /// </summary>
        /// <param name="remote">The tcpclient to be wrapped</param>
        public IoNetClient(IoSocket remote)
        {
            _remoteSocket = remote;
            _logger = LogManager.GetCurrentClassLogger();
            _hostname = _remoteSocket.RemoteAddress?.ToString() ?? "N/A";
            _port = _remoteSocket?.RemotePort ?? 0;
        }

        /// <summary>
        /// Constructor for connecting
        /// </summary>
        /// <param name="hostname">The hostname to connect to</param>
        /// <param name="port">The listening port</param>
        public IoNetClient(string hostname, int port)
        {
            _hostname = hostname;
            _port = port;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// hostname
        /// </summary>
        private readonly string _hostname;

        /// <summary>
        /// port
        /// </summary>
        private readonly int _port;

        /// <summary>
        /// A description of this client. Currently the remote address
        /// </summary>
        protected override string Description => Address;

        /// <summary>
        /// The tcpclient that is being wrapped
        /// </summary>
        private IoSocket _remoteSocket;

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
        /// Transmit buffer size
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_tx_buffer_size = 1650 * 10;

        /// <summary>
        /// Receive buffer size
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_rx_buffer_size;

        /// <summary>
        /// Called when disconnect is detected
        /// </summary>
        public event EventHandler Disconnected;

        private readonly CancellationTokenSource _spinners = new CancellationTokenSource(); //TODO hook this up
        private CancellationTokenRegistration _cancellationTokenRegistration;


        //Is the client rawSocket still up?
        public bool Connected => _remoteSocket?.NativeSocket?.Connected??false;

        /// <summary>
        /// Closes the connection
        /// </summary>
        public void Close()
        {
            Spinners.Cancel();

            OnDisconnected();            
            
            _remoteSocket?.Close();
            _remoteSocket = null;
            
            //Unlock any blockers
            ConsumeSemaphore?.Dispose();
            ProduceSemaphore?.Close();
            
            _logger.Debug($"Closed connection `{Address}'");
        }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>The async task handler</returns>
        public async Task ConnectAsync()
        {
            _remoteSocket?.Close();

            //create a new rawSocket
            _remoteSocket = IoSocket.GetKindFromUrl(_hostname, _spinners.Token);

            _cancellationTokenRegistration = Spinners.Token.Register(() => _remoteSocket?.Spinners.Cancel());            

            //_remoteSocket.NativeSocket.SendTimeout = parm_tx_timeout;
            //_remoteSocket.NativeSocket.ReceiveTimeout = parm_rx_timeout;
            //_remoteSocket.NativeSocket.ReceiveBufferSize = parm_tx_buffer_size;
            //_remoteSocket.NativeSocket.SendBufferSize = parm_rx_buffer_size;
            if(_remoteSocket.NativeSocket.ProtocolType == ProtocolType.Tcp)
                _remoteSocket.NativeSocket.LingerState = new LingerOption(true, 1);

            _logger.Info($"Connecting to `{Address}'");
            
            //connect to the remote rawSocket
            await _remoteSocket.ConnectAsync(IoSocket.StripIp(_hostname) , _port).ContinueWith(_ =>
                {
                    _remoteSocket.Disconnected += (s, e) => _cancellationTokenRegistration.Dispose();
                });
        }

        /// <summary>
        /// Execute the a tcp client function, detect TCP connection drop
        /// </summary>
        /// <param name="callback">The tcp client functions</param>
        /// <returns>True on success, false otherwise</returns>
        public async Task<bool> Execute(Func<IoSocket, Task<bool>> callback)
        {
            //Is the TCP connection up?
            if (!Up())//TODO fix up
                return false;
            try
            {
                return await callback(_remoteSocket);
            }
            catch
            {
                //Did the TCP connection drop?
                Up();
                throw;
            }
        }

        /// <summary>
        /// Emit disconnect event
        /// </summary>
        protected virtual void OnDisconnected()
        {
            Disconnected?.Invoke(this, new EventArgs());
        }

        /// <summary>
        /// Detects TCP connection drops
        /// </summary>
        /// <returns>True it the connection is up, false otherwise</returns>
        public bool Up()
        {
            try
            {
                
                if (_remoteSocket?.NativeSocket != null && _remoteSocket.NativeSocket.ProtocolType == ProtocolType.Tcp )
                {
                    //var selectError = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectError)?"FAILED":"OK";
                    //var selectRead = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectRead)? "OK" : "FAILED";//TODO what is this?
                    //var selectWrite = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectWrite)? "OK" : "FAILED";

                    //TODO more checks?
                    if (!_remoteSocket.NativeSocket
                        .Connected /*|| selectError=="FAILED" || selectRead == "FAILED" || selectWrite == "FAILED" */)
                    {
                        //_logger.Warn($"`{Address}' is in a faulted state, connected={_ioNetClient.Client.Connected}, {SelectMode.SelectError}={selectError}, {SelectMode.SelectRead}={selectRead}, {SelectMode.SelectWrite}={selectWrite}");
                        _logger.Warn($"Connection to `{Address}' disconnected!");

                        //Do cleanup
                        Close();

                        return false;
                    }

                    return true;
                }
                else
                    return true;
            }
            catch (Exception e)
            {
                _logger.Error(e, $"The connection to `{Description}' has been closed:");
                return false;
            }
        }

        /// <summary>
        /// Returns the host address URL in the format tcp://IP:port
        /// </summary>
        public string Address => $"{_hostname}:{_port}";
    }
}
