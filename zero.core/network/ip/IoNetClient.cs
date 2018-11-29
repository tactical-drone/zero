using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Tangle.Net.Entity;
using zero.core.conf;
using zero.core.models;
using zero.core.patterns;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// Wraps a <see cref="TcpClient"/> into a <see cref="IoProducer{TJob}"/> that can be used by <see cref="IoProducerConsumer{TJob}"/>
    /// </summary>
    public class IoNetClient<TJob> : IoProducer<TJob>
    where TJob : IIoWorker
    
    {
        /// <summary>
        /// Constructor for listening
        /// </summary>
        /// <param name="remote">The tcpclient to be wrapped</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoNetClient(IoSocket remote, int readAhead) : base(readAhead)
        {
            IoSocket = (IoNetSocket)remote;
            _logger = LogManager.GetCurrentClassLogger();
            Address = remote.Address;
        }

        /// <summary>
        /// Constructor for connecting
        /// </summary>
        /// <param name="address">The address associated with this network client</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoNetClient(IoNodeAddress address, int readAhead) : base(readAhead)
        {
            Address = address;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The remote address associated with this client
        /// </summary>
        protected readonly IoNodeAddress Address;

        public override IoForward<TFJob> GetForwardProducer<TFJob>(IoProducer<TFJob> producer = null, Func<object, IoConsumable<TFJob>> mallocMessage = null)
        {
            if (IoForward == null)
            {
               if( producer == null || mallocMessage == null)
                    _logger.Warn($"Waiting for the multicast producer of `{Description}' to initialize...");

                IoForward = new IoForward<TFJob>(Description, producer, mallocMessage);
            }
               
            return (IoForward<TFJob>) IoForward;
        }

        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>
        /// The unique key of this instance
        /// </returns>
        public override int Key => IoSocket?.LocalPort??-1;

        /// <summary>
        /// A description of this client. Currently the remote address
        /// </summary>
        public override string Description => AddressString;

        /// <summary>
        /// Abstracted dotnet udp and tcp socket
        /// </summary>
        protected IoNetSocket IoSocket;

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

        /// <summary>
        /// Cancellation hooks
        /// </summary>
        private readonly CancellationTokenSource _spinners = new CancellationTokenSource(); //TODO hook this up

        /// <summary>
        /// Handle to unregister cancellation registrations
        /// </summary>
        private CancellationTokenRegistration _cancellationRegistratison;

        /// <summary>
        /// This is a temporary sync hack for TCP. Sometimes IRI has old data stuck in it's TCP stack that has to be flushed.
        /// We do this by waiting for IRI to send us exactly the right amount of data. There is a better way but this will do for now
        /// Until we can troll the data for verified hashes, which will be slower but more accurate.
        /// </summary>
        public bool TcpSynced = false;

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        public override bool IsOperational => IsSocketConnected();

        /// <summary>
        /// Closes the connection
        /// </summary>
        public override void Close()
        {
            Spinners.Cancel();

            OnDisconnected();

            IoSocket?.Close();
            IoSocket = null;

            //Unlock any blockers
            ProducerBarrier?.Dispose();
            ConsumerBarrier?.Dispose();

            _logger.Debug($"Closed connection `{AddressString}'");
        }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>True if succeeded, false otherwise</returns>
        public virtual async Task<bool> ConnectAsync()
        {
            var connectAsyncTask = IoSocket.ConnectAsync(Address);

            _logger.Debug($"Connecting to `{Address}'");

            return await connectAsyncTask.ContinueWith(t =>
            {
                if (t.Result)
                {
                    _cancellationRegistratison = Spinners.Token.Register(() => IoSocket?.Spinners.Cancel());

                    IoSocket.Disconnected += (s, e) => _cancellationRegistratison.Dispose();

                    _logger.Info($"Connected to `{AddressString}'");
                }
                return connectAsyncTask;
            }).Unwrap();
        }

        /// <summary>
        /// Execute the a tcp client function, detect TCP connection drop
        /// </summary>
        /// <param name="callback">The tcp client functions</param>
        /// <returns>True on success, false otherwise</returns>


        //public async Task<Task> Execute(Func<IoSocket, Task<Task>> callback)
        public override async Task<Task> Produce(Func<IIoProducer, Task<Task>> callback)
        {
            //Is the TCP connection up?
            if (!IsOperational) //TODO fix up
                throw new IOException("Socket has disconnected");

            try
            {
                return await callback(IoSocket);
            }
            catch (Exception e)
            {
                //Did the TCP connection drop?
                if (!IsSocketConnected())
                    throw new IOException("Socket has disconnected", e);
                throw; //TODO fix this
            }
        }

        /// <summary>
        /// Emit disconnect event
        /// </summary>
        public virtual void OnDisconnected()
        {
            Disconnected?.Invoke(this, new EventArgs());
        }

        /// <summary>
        /// Detects socket drops //TODO this needs some work or testing
        /// </summary>
        /// <returns>True it the connection is up, false otherwise</returns>
        public bool IsSocketConnected()
        {
            try
            {
                if (IoSocket?.NativeSocket != null && IoSocket.IsTcpSocket)
                {
                    //var selectError = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectError)?"FAILED":"OK";
                    //var selectRead = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectRead)? "OK" : "FAILED";//TODO what is this?
                    //var selectWrite = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectWrite)? "OK" : "FAILED";

                    //TODO more checks?
                    if (!IoSocket.Connected() /*|| selectError=="FAILED" || selectRead == "FAILED" || selectWrite == "FAILED" */)
                    {
                        //_logger.Warn($"`{Address}' is in a faulted state, connected={_ioNetClient.Client.Connected}, {SelectMode.SelectError}={selectError}, {SelectMode.SelectRead}={selectRead}, {SelectMode.SelectWrite}={selectWrite}");
                        _logger.Warn($"Connection to `{AddressString}' disconnected!");

                        //Do cleanup
                        Close();

                        return false;
                    }

                    return true;
                }
                else
                {
                    return IoSocket?.Connected() ?? false;
                }
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
        public string AddressString => Address.ToString();
    }
}
