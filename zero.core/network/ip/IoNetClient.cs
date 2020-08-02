using System;
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
    /// Wraps a <see cref="TcpClient"/> into a <see cref="IoProducer{TJob}"/> that can be used by <see cref="IoProducerConsumer{TJob}"/>
    /// </summary>
    public abstract class IoNetClient<TJob> : IoProducer<TJob>
    where TJob : IIoWorker
    
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoNetClient{TJob}"/> class.
        /// </summary>
        protected IoNetClient()
        {
            
        }

        /// <summary>
        /// Constructor for incoming connections used by the listener
        /// </summary>
        /// <param name="remote">The remote socket</param>
        /// <param name="readAheadBufferSize">The amount of socket reads the producer is allowed to lead the consumer</param>
        protected IoNetClient(IoSocket remote,int readAheadBufferSize) : base(readAheadBufferSize)
        {
            IoSocket = (IoNetSocket)remote;
            _logger = LogManager.GetCurrentClassLogger();
            ListenerAddress = remote.ListenerAddress;                        
        }

        /// <summary>
        /// Constructor for connecting
        /// </summary>
        /// <param name="listenerAddress">The address associated with this network client</param>
        /// <param name="arbiter">The job arbitrator</param>
        /// <param name="readAheadBufferSize">The amount of socket reads the producer is allowed to lead the consumer</param>
        protected IoNetClient(IoNodeAddress listenerAddress, int readAheadBufferSize) : base(readAheadBufferSize)
        {
            ListenerAddress = listenerAddress;
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The remote address associated with this client
        /// </summary>
        public readonly IoNodeAddress ListenerAddress;

        /// <summary>
        /// The client remote address
        /// </summary>
        public IoNodeAddress RemoteAddress => IoSocket.RemoteAddress;

        /// <summary>
        /// Configure a downstream producer
        /// </summary>
        /// <typeparam name="TFJob">The id of the downstream producer</typeparam>
        /// <param name="id">The id of the downstream producer</param>
        /// <param name="producer">The downstream producer</param>
        /// <param name="jobMalloc">Allocates jobs</param>
        /// <returns><see cref="IoForward{TJob}"/> worker</returns>
        public override IoForward<TFJob> GetDownstreamArbiter<TFJob>(string id, IoProducer<TFJob> producer = null,
            Func<object, IoConsumable<TFJob>> jobMalloc = null)
        {
            if (!IoForward.ContainsKey(id))
            {
                if (producer == null || jobMalloc == null)
                {
                    _logger.Warn($"Waiting for the multicast producer of `{Description}' to initialize...");
                    return null;
                }

                lock (this)
                {
                    IoForward.TryAdd(id, new IoForward<TFJob>(Description, producer, jobMalloc));                    
                    producer.ConfigureUpstream(this);
                    producer.SetArbiter((IoProducerConsumer<TFJob>)IoForward[id]);
                }                
            }
               
            return (IoForward<TFJob>) IoForward[id];
        }

        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>
        /// The unique key of this instance
        /// </returns>
        public override string Key => IoSocket.Key;

        /// <summary>
        /// A description of this client. Currently the remote address
        /// </summary>
        public override string Description => $"{IoSocket?.RemoteAddress?.ToString()??ListenerAddress.ToString()}";

        /// <summary>
        /// A description of this client source. Currently the remote address
        /// </summary>
        public override string SourceUri => $"{IoSocket.ListenerAddress.ProtocolDesc}{IoSocket.RemoteIpAndPort}";

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
        public string AddressString => ListenerAddress.ResolvedIpAndPort;


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
        private CancellationTokenRegistration _cancellationRegistration;
        
        /// <summary>
        /// Closes the connection
        /// </summary>
        public override void Close()
        {
            lock (this)
            {
                if (Closed) return;
                Closed = true;
            }
            
            _logger.Debug($"Closing `{Description}'");

            OnDisconnected();

            Spinners?.Cancel();
            
            IoSocket?.Close();
            //IoSocket = null;

            //Unlock any blockers
            ProducerBarrier?.Dispose();
            ConsumerBarrier?.Dispose();
            ConsumeAheadBarrier?.Dispose();
            ProduceAheadBarrier?.Dispose();            
        }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>True if succeeded, false otherwise</returns>
        public virtual async Task<bool> ConnectAsync()
        {            
            var connectAsyncTask = IoSocket.ConnectAsync(ListenerAddress);            

            _logger.Debug($"Connecting to `{ListenerAddress}'");
            
            return await connectAsyncTask.ContinueWith(t =>
            {
                if (t.Result)
                {
                    _cancellationRegistration = Spinners.Token.Register(() => IoSocket?.Spinners.Cancel());

                    IoSocket.Disconnected += (s, e) => _cancellationRegistration.Dispose();

                    _logger.Info($"Connected to `{AddressString}'");                    
                }
                else
                {
                    _logger.Debug($"Failed to connect to `{AddressString}'");
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
        public override async Task<bool> ProduceAsync(Func<IIoProducer, Task<bool>> callback)
        {
            //Is the TCP connection up?
            if (!IsOperational)
            {
                return false;
            }                

            try
            {
                return await callback(IoSocket);
            }
            catch (TimeoutException)
            {
                return false;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Producer `{Description}' callback failed:");
                return false;
            }
        }

        public override void ConfigureUpstream(IIoProducer producer)
        {
            Upstream = producer;            
        }

        public override void SetArbiter(IoProducerConsumer<TJob> arbiter)
        {
            Arbiter = arbiter;
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
        public override bool IsOperational
        {
            get
            {
                try
                {
                    if (IoSocket?.NativeSocket != null && IoSocket.IsTcpSocket)
                    {
                        //var selectError = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectError)?"FAILED":"OK";
                        //var selectRead = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectRead)? "OK" : "FAILED";//TODO what is this?
                        //var selectWrite = _ioNetClient.Client.Poll(IoConstants.parm_rx_timeout, SelectMode.SelectWrite)? "OK" : "FAILED";

                        //TODO more checks?
                        if (!IoSocket.IsConnected() /*|| selectError=="FAILED" || selectRead == "FAILED" || selectWrite == "FAILED" */)
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
