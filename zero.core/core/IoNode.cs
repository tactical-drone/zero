using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.data.market;
using zero.core.data.providers.redis.configurations.tangle;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.core.core
{
    /// <summary>
    /// A p2p node
    /// </summary>
    public class IoNode<TJob> : IoConfigurable        
    where TJob:IIoWorker    
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoNode(IoNodeAddress address, Func<IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead)
        {
            _address = address;
            _mallocNeighbor = mallocNeighbor;
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
        private readonly Func<IoNetClient<TJob>, IoNeighbor<TJob>> _mallocNeighbor;

        /// <summary>
        /// The wrapper for <see cref="IoNetServer"/>
        /// </summary>
        private IoNetServer<TJob> _netServer;

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
        private readonly CancellationTokenSource _spinners = new CancellationTokenSource();

        /// <summary>
        /// On Connected
        /// </summary>
        public EventHandler<IoNeighbor<TJob>> PeerConnected;

        /// <summary>
        /// 
        /// </summary>
        public EventHandler<IoNeighbor<TJob>> PeerDisconnected;


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

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async Task SpawnListenerAsync()
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _spinners.Token, parm_tcp_readahead);

            await _netServer.StartListenerAsync(async remoteClient =>
            {
                var newNeighbor = _mallocNeighbor(remoteClient);

                // Register close hooks
                var cancelRegistration = _spinners.Token.Register(() =>
                {
                    newNeighbor.Close();
                });

                //Close neighbor when connection closes
                remoteClient.Disconnected += (s, e) =>
                {
                    newNeighbor.Close();
                };

                // Remove from lists if closed
                newNeighbor.Closed += (s, e) =>
                {
                    cancelRegistration.Dispose();
                    PeerDisconnected?.Invoke(this, newNeighbor);
                    Neighbors.TryRemove(((IoNeighbor<TJob>)s).PrimaryProducer.Key, out var _);
                };

                // Add new neighbor
                if (!Neighbors.TryAdd(remoteClient.Key, newNeighbor))
                {
                    newNeighbor.Close();
                    _logger.Warn($"Neighbor `{remoteClient.ListenerAddress}' already connected. Possible spoof investigate!");
                }

                //New peer connection event
                PeerConnected?.Invoke(this, newNeighbor);

                //start redis
                var connectTask =  IoTangleTransactionHashCache.Default();
                                        
                await connectTask.ContinueWith(r =>
                {
                    switch (r.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            break;
                        case TaskStatus.RanToCompletion:
                            remoteClient.RecentlyProcessed = r.Result;
                            break;
                    }
                });                

                //Start the producer consumer on the neighbor scheduler
                try
                {
                    Task.Factory.StartNew(() => newNeighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Neighbor `{newNeighbor.PrimaryProducer.Description}' processing thread returned with errors:");
                }
            }, parm_tcp_readahead);
        }

        /// <summary>
        /// Make sure a connection stays up
        /// </summary>        
        /// <param name="address">The remote node address</param>
        /// <param name="retry">Retry on failure</param>
        /// <param name="retryTimeoutMs">Retry timeout in ms</param>
        /// <returns>The async task</returns>
        public async Task<IoNeighbor<TJob>> SpawnConnectionAsync(IoNodeAddress address, bool retry = false, int retryTimeoutMs = 10000)
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
                        var neighbor = newNeighbor = _mallocNeighbor(newClient);
                        _spinners.Token.Register(() => neighbor.Spinners.Cancel());

                        if (Neighbors.TryAdd(newNeighbor.PrimaryProducer.Key, newNeighbor))
                        {
                            neighbor.parm_producer_start_retry_time = 60000;
                            neighbor.parm_consumer_wait_for_producer_timeout = 60000;

                            newNeighbor.Closed += (s, e) =>
                            {
                                if (!Neighbors.TryRemove(((IoNeighbor<TJob>)s).PrimaryProducer.Key, out _))
                                {
                                    _logger.Fatal($"Neighbor metadata expected for key `{newNeighbor.PrimaryProducer.Key}'");
                                }
                            };
                            
                            return newNeighbor;
                        }
                        else //strange case
                        {
                            newNeighbor.Close();
                            newNeighbor = null;
                        }

                        connectedAtLeastOnce = true;
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
        public void Start()
        {
            _logger.Info("Unimatrix Zero");
            try
            {
#pragma warning disable 4014
                SpawnListenerAsync().ContinueWith(_=> _logger.Info("You will be assimilated!"));
#pragma warning restore 4014                
            }
            catch (Exception e)
            {
                _logger.Error(e, "Unimatrix Zero returned with errors: ");
            }
        }

        /// <summary>
        /// Stop the node
        /// </summary>
        public void Stop()
        {
            _spinners.Cancel();
            Neighbors.ToList().ForEach(n => n.Value.Close());
            Neighbors.Clear();
            _logger.Info("Resistance is futile");
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
                Neighbors.Values.Where(n=>n.PrimaryProducer.Key.Contains(address.ProtocolDesc)).ToList().ForEach(n =>
                {                    
                    keys.Add(n.PrimaryProducer.Key);
                });

                Neighbors[address.ToString()].Close();
                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
