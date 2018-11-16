using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.models;
using zero.core.patterns.bushes;

namespace zero.core.network.ip
{
    /// <summary>
    /// Wraps a <see cref="TcpClient"/> into a <see cref="IoJobSource"/> that can be used by <see cref="IoProducerConsumer{TJob,TSource}"/>
    /// </summary>
    public class IoNetClient: IoJobSource
    {
        /// <summary>
        /// Conctructor for listening
        /// </summary>
        /// <param name="remote">The tcpclient to be wrapped</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoNetClient(IoSocket remote, int readAhead):base(readAhead)
        {
            _remoteSocket = remote;
            _logger = LogManager.GetCurrentClassLogger();
            _address = IoNodeAddress.Create(_remoteSocket.RemoteAddress ?? _remoteSocket.LocalAddress, _remoteSocket.RemoteAddress != null? _remoteSocket.RemotePort: _remoteSocket.LocalPort);            
        }

        /// <summary>
        /// Constructor for connecting
        /// </summary>
        /// <param name="address">The address associated with this network client</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoNetClient(IoNodeAddress address, int readAhead):base(readAhead)
        {
            _address = address;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The remote address associated with this client
        /// </summary>
        private readonly IoNodeAddress _address;

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
        /// Called when disconnect is detected
        /// </summary>
        public event EventHandler Disconnected;

        private readonly CancellationTokenSource _spinners = new CancellationTokenSource(); //TODO hook this up
        private CancellationTokenRegistration _cancellationTokenRegistration;


        //Is the client rawSocket still up?
        public bool Connected => _remoteSocket?.NativeSocket?.Connected??false;

        /// <summary>
        /// A flag to indicate if those pesky extra tcp bits are contained in the datum
        /// </summary>
        public bool ContainsExtrabits => _remoteSocket?.NativeSocket.ProtocolType == ProtocolType.Tcp;

        /// <summary>
        /// This is a temporary sync hack for TCP. Sometimes IRI has old data stuck in it's TCP stack that has to be flushed.
        /// We do this by waiting for IRI to send us exactly the right amount of data. There is a better way but this will do for now
        /// Until we can troll the data for verified hashes, which will be slower but more accurate.
        /// </summary>
        public bool TcpSynced = false;

        /// <summary>
        /// Closes the connection
        /// </summary>
        public override void Close()
        {
            Spinners.Cancel();

            OnDisconnected();            
            
            _remoteSocket?.Close();
            _remoteSocket = null;
            
            //Unlock any blockers
            ProducerBarrier?.Dispose();
            ConsumerBarrier?.Dispose();
            
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
            _remoteSocket = IoSocket.GetKindFromUrl(_address.Url, _spinners.Token);

            _cancellationTokenRegistration = Spinners.Token.Register(() => _remoteSocket?.Spinners.Cancel());            

            //_remoteSocket.NativeSocket.SendTimeout = parm_tx_timeout;
            //_remoteSocket.NativeSocket.ReceiveTimeout = parm_rx_timeout;
            //_remoteSocket.NativeSocket.ReceiveBufferSize = parm_tx_buffer_size;
            //_remoteSocket.NativeSocket.SendBufferSize = parm_rx_buffer_size;
            if(_remoteSocket.NativeSocket.ProtocolType == ProtocolType.Tcp)
                _remoteSocket.NativeSocket.LingerState = new LingerOption(true, 1);

            _logger.Info($"Connecting to `{Address}'");
            
            //connect to the remote rawSocket
            await _remoteSocket.ConnectAsync(IoSocket.StripIp(_address.Url) , _address.Port).ContinueWith(_ =>
                {
                    _remoteSocket.Disconnected += (s, e) => _cancellationTokenRegistration.Dispose();
                });
        }

        /// <summary>
        /// Execute the a tcp client function, detect TCP connection drop
        /// </summary>
        /// <param name="callback">The tcp client functions</param>
        /// <returns>True on success, false otherwise</returns>
        public async Task<Task> Execute(Func<IoSocket, Task<Task>> callback)
        {
            //Is the TCP connection up?
            if (!IsSocketConnected()) //TODO fix up
                throw new IOException("Socket has disconnected");

            try
            {
                return await callback(_remoteSocket);
            }
            catch(Exception e)
            {
                //Did the TCP connection drop?
                if (!IsSocketConnected())
                    throw new IOException("Socket has disconnected", e);
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
        /// Detects socket drops
        /// </summary>
        /// <returns>True it the connection is up, false otherwise</returns>
        public bool IsSocketConnected()
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
        public string Address => _address.ToString();        
    }
}
