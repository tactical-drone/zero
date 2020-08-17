﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
        private readonly CancellationTokenSource _spinners = new CancellationTokenSource();

        protected CancellationToken CancellationToken => _spinners.Token;

        /// <summary>
        /// On Connected
        /// </summary>
        public EventHandler<IoNeighbor<TJob>> ConnectedEvent;

        /// <summary>
        /// 
        /// </summary>
        public EventHandler<IoNeighbor<TJob>> DisconnectedEvent;


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
        protected virtual async Task SpawnListenerAsync(Action<IoNeighbor<TJob>> connectionReceivedAction = null)
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _spinners.Token, parm_tcp_readahead);

            await _netServer.StartListenerAsync(remoteClient =>
            {
                var newNeighbor = MallocNeighbor(this, remoteClient, null);

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
                    DisconnectedEvent?.Invoke(this, newNeighbor);
                    Neighbors.TryRemove(((IoNeighbor<TJob>)s).Source.Key, out var _);
                };

                // Add new neighbor
                if (!Neighbors.TryAdd(remoteClient.Key, newNeighbor))
                {
                    newNeighbor.Close();
                    _logger.Warn($"Neighbor `{remoteClient.ListeningAddress}' already connected. Possible spoof investigate!");
                }

                //New peer connection event
                ConnectedEvent?.Invoke(this, newNeighbor);

                //super class specific mutations
                connectionReceivedAction?.Invoke(newNeighbor);

                //start redis                

                //Start the source consumer on the neighbor scheduler
                try
                {                    
#pragma warning disable 4014
                    Task.Factory.StartNew(() => newNeighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);
#pragma warning restore 4014
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Neighbor `{newNeighbor.Source.Description}' processing thread returned with errors:");
                }
            }, parm_tcp_readahead);
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
                        
                        _spinners.Token.Register(() => neighbor.Spinners.Cancel());

                        //TODO does this make sense?
                        if (Neighbors.ContainsKey(newNeighbor.Id))
                        {
                            var n = Neighbors[neighbor.Id];
                            n.Close();
                        }

                        if (Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                        {
                            neighbor.parm_producer_start_retry_time = 60000;
                            neighbor.parm_consumer_wait_for_producer_timeout = 60000;

                            newNeighbor.Closed += (s, e) =>
                            {
                                if (!Neighbors.TryRemove(newNeighbor.Id, out _))
                                {
                                    _logger.Fatal($"Neighbor metadata expected for key `{newNeighbor.Id}'");
                                }
                            };
                            
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
            _logger.Info($"Unimatrix Zero - {ToString()}");
            try
            {
                await SpawnListenerAsync().ContinueWith(_=> _logger.Info($"You will be assimilated! - {ToString()}"), CancellationToken);
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
    }
}
