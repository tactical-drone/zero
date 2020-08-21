using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.data.market;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.core.core
{
    /// <summary>
    /// A p2p node
    /// </summary>
    public class IoNode<TJob> : IoConfigurable        
    where TJob:IIoJob    
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, object , IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead)
        {
            _address = address;
            MallocNeighbor = mallocNeighbor;
            parm_tcp_readahead = tcpReadAhead;            
            _logger = LogManager.GetCurrentClassLogger();
            var q = IoMarketDataClient.Quality;//prime market data            
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The listening address of this node
        /// </summary>
        private readonly IoNodeAddress _address;

        /// <summary>
        /// Used to allocate peers when connections are made
        /// </summary>
        public Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> MallocNeighbor { get; protected set; }

        /// <summary>
        /// The wrapper for <see cref="IoNetServer"/>
        /// </summary>
        private IoNetServer<TJob> _netServer;

        /// <summary>
        /// The server
        /// </summary>
        public IoNetServer<TJob> Server => _netServer;

        /// <summary>
        /// All the neighbors connected to this node
        /// </summary>
        public readonly ConcurrentDictionary<string, IoNeighbor<TJob>> Neighbors = new ConcurrentDictionary<string, IoNeighbor<TJob>>();

        /// <summary>
        /// Allowed clients
        /// </summary>
        private readonly ConcurrentDictionary<string, IoNodeAddress> _whiteList = new ConcurrentDictionary<string, IoNodeAddress>();

        /// <summary>
        /// Used to cancel downstream processes
        /// </summary>
        protected readonly CancellationTokenSource _spinners = new CancellationTokenSource();

        protected CancellationToken CancellationToken => _spinners.Token;

        /// <summary>
        /// On Connected
        /// </summary>
        protected EventHandler<IoNeighbor<TJob>> ConnectedEvent;

        /// <summary>
        /// 
        /// </summary>
        protected EventHandler<IoNeighbor<TJob>> DisconnectedEvent;

        /// <summary>
        /// Threads per neighbor
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_max_neighbor_pc_threads = 3;

        /// <summary>
        /// TCP read ahead
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_tcp_readahead = 2;

        private Task _listernerTask;

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async Task SpawnListenerAsync(Func<IoNeighbor<TJob>, Task<bool>> acceptConnection = null)
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, parm_tcp_readahead);
            _netServer.ClosedEvent((sender, args) => Close());

            await _netServer.ListenAsync(async client =>
            {
                var newNeighbor = MallocNeighbor(this, client, null);

                //superclass specific mutations
                try
                {
                    if (acceptConnection != null && !await acceptConnection.Invoke(newNeighbor))
                    {
                        _logger.Debug($"Incoming connection from {client.Key} rejected.");
                        newNeighbor.Close();
                        return;
                    }
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Accepting connection {client.Key} returned with errors");
                    return;
                }

                // Remove from lists if closed
                newNeighbor.ClosedEvent((s, e) =>
                {
                    DisconnectedEvent?.Invoke(this, newNeighbor);
                    if (Neighbors.TryRemove(((IoNeighbor<TJob>) s)?.Id, out var _))
                        _logger.Debug($"Removed neighbor Id = {((IoNeighbor<TJob>)s)?.Id}");
                });

                // Add new neighbor
                if (!Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                {
                    newNeighbor.Close();
                    _logger.Warn($"Neighbor `{client.ListeningAddress}' already connected. Possible spoof investigate!");
                }

                //New peer connection event
                //ConnectedEvent?.Invoke(this, newNeighbor);

                //Start the source consumer on the neighbor scheduler
                try
                {                    
#pragma warning disable 4014
                    Task.Factory.StartNew(() => newNeighbor.SpawnProcessingAsync(), _spinners.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);
#pragma warning restore 4014
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Neighbor `{newNeighbor.Source.Description}' processing thread returned with errors:");
                }
            }, parm_tcp_readahead).ConfigureAwait(false);
        }

        /// <summary>
        /// Make sure a connection stays up
        /// </summary>        
        /// <param name="address">The remote node address</param>
        /// <param name="extraData">Any extra data you want to send to the neighbor constructor</param>
        /// <param name="retry">Retry on failure</param>
        /// <param name="retryTimeoutMs">Retry timeout in ms</param>
        /// <returns>The async task</returns>
        public async Task<IoNeighbor<TJob>> SpawnConnectionAsync(IoNodeAddress address, object extraData = null, bool retry = false, int retryTimeoutMs = 10000)
        {
            IoNeighbor<TJob> newNeighbor = null;
            bool connectedAtLeastOnce = false;

            while (!_spinners.IsCancellationRequested && !connectedAtLeastOnce)
            {
                if (newNeighbor == null && !connectedAtLeastOnce)
                {
                    var newClient = await _netServer.ConnectAsync(address);

                    if (newClient != null && newClient.IsOperational)
                    {
                        var neighbor = newNeighbor = MallocNeighbor(this, newClient, extraData);

                        //TODO does this make sense?
                        if (Neighbors.ContainsKey(newNeighbor.Id))
                        {
                            var n = Neighbors[neighbor.Id];
                            n.Close();
                        }

                        if (Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                        {
                            //TODO
                            neighbor.parm_producer_start_retry_time = 60000;
                            neighbor.parm_consumer_wait_for_producer_timeout = 60000;

                            newNeighbor.ClosedEvent((s, e) =>
                            {
                                if (!Neighbors.TryRemove(newNeighbor.Id, out _))
                                {
                                    _logger.Fatal($"Neighbor metadata expected for key `{newNeighbor.Id}'");
                                }
                            });
                            
                            _logger.Info($"Added {newNeighbor.Id}");

                            ConnectedEvent?.Invoke(this, newNeighbor);

                            return newNeighbor;
                        }
                        else //strange case
                        {
                            _logger.Fatal($"Neighbor with id = {newNeighbor.Id} already exists! Closing connection...");
                            newNeighbor.Close();
                            newNeighbor = null;
                        }

                        connectedAtLeastOnce = true;
                    }
                    else
                    {
                        _logger.Error($"Failed to connect to: {address}, {address.ValidationErrorString}");
                    }
                }
                else
                {
                    //TODO param
                    await Task.Delay(retryTimeoutMs);
                }                

                if(!retry)
                    break;
            }

            return null;
        }

        /// <summary>
        /// Start the node
        /// </summary>
        public async Task StartAsync()
        {
            _logger.Info($"Unimatrix Zero - Launching cube: {ToString()}");
            try
            {
                _listernerTask = SpawnListenerAsync();
                await _listernerTask.ContinueWith(_=> _logger.Info($"You will be assimilated! - {ToString()} ({_.Status})"), CancellationToken).ConfigureAwait(false);

                _logger.Info($"{ToString()}: Resistance is futile, {(_listernerTask.GetAwaiter().IsCompleted ? "clean" : "dirty")} exit ({_listernerTask.Status})");
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unimatrix Zero returned {ToString()}");
            }
        }

        /// <summary>
        /// Closed or not
        /// </summary>
        protected volatile bool Closed = false;

        /// <summary>
        /// Emits closed events
        /// </summary>
        protected event EventHandler __closedEvent;

        /// <summary>
        /// Keeps tabs on all subscribers to be freed on close
        /// </summary>
        private readonly ConcurrentBag<EventHandler> _closedEventHandlers = new ConcurrentBag<EventHandler>();

        /// <summary>
        /// Enables safe subscriptions to close events
        /// </summary>
        /// <param name="del"></param>
        public void ClosedEvent(EventHandler del)
        {
            _closedEventHandlers.Add(del);
            __closedEvent += del;
        }

        /// <summary>
        /// Close the node
        /// </summary>
        public virtual bool Close()
        {
            lock (this)
            {
                if (Closed) return false;
                Closed = true;
            }

            _logger.Info($"{ToString()}: Closing {Server.Description}");

            OnClosedEvent();

            Neighbors.ToList().ForEach(n => n.Value.Close());
            Neighbors.Clear();

            _netServer.Close();

            _spinners.Cancel();
            return true;
        }
        
        public bool WhiteList(IoNodeAddress address)
        {
            if (_whiteList.TryAdd(address.ToString(), address))
            {
                _logger.Error($"Unable to add `{address}', key `{address}' already exists!");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Blacklists a neighbor
        /// </summary>
        /// <param name="address">The address of the neighbor</param>
        /// <returns>The blacklisted neighbor if it was connected</returns>
        public IoNeighbor<TJob> BlackList(IoNodeAddress address)
        {
            if (_whiteList.TryRemove(address.ToString(), out var ioNodeAddress))
            {
                var keys = new List<string>();
                Neighbors.Values.Where(n=>n.Source.Key.Contains(address.ProtocolDesc)).ToList().ForEach(n =>
                {                    
                    keys.Add(n.Source.Key);
                });

                Neighbors[address.ToString()].Close();
                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }

        /// <summary>
        /// Emits closed event
        /// </summary>
        protected virtual void OnClosedEvent()
        {
            __closedEvent?.Invoke(this, EventArgs.Empty);

            //free handles
            _closedEventHandlers.ToList().ForEach(del => __closedEvent -= del);
            _closedEventHandlers.Clear();
        }
    }
}
