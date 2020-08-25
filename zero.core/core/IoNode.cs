﻿using System;
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
        private IoNodeAddress _address;

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
        public ConcurrentDictionary<string, IoNeighbor<TJob>> Neighbors = new ConcurrentDictionary<string, IoNeighbor<TJob>>();

        /// <summary>
        /// Allowed clients
        /// </summary>
        private ConcurrentDictionary<string, IoNodeAddress> _whiteList = new ConcurrentDictionary<string, IoNodeAddress>();

        /// <summary>
        /// On Connected
        /// </summary>
        //protected EventHandler<IoNeighbor<TJob>> ConnectedEvent;

        /// <summary>
        /// 
        /// </summary>
        //protected EventHandler<IoNeighbor<TJob>> DisconnectedEvent;

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
        /// 
        /// </summary>
        private Task _listenerTask;

        /// <summary>
        /// A set of all node tasks that are currently running
        /// </summary>
        protected ConcurrentBag<Task> NeighborTasks = new ConcurrentBag<Task>();

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async Task SpawnListenerAsync(Func<IoNeighbor<TJob>, Task<bool>> acceptConnection = null)
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, parm_tcp_readahead);
            _netServer.ZeroOnCascade(this, true);

            await _netServer.ListenAsync(async ioNetClient =>
            {
                var newNeighbor = MallocNeighbor(this, ioNetClient, null);

                //superclass specific mutations
                try
                {
                    if (acceptConnection != null && !await acceptConnection.Invoke(newNeighbor).ConfigureAwait(false))
                    {
                        _logger.Debug($"Incoming connection from {ioNetClient.Key} rejected.");
#pragma warning disable 4014
                        newNeighbor.Zero(this);
#pragma warning restore 4014
                        return;
                    }
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Accepting connection {ioNetClient.Key} returned with errors");
                    return;
                }

                //We use this locally captured variable as newNeighbor.Id disappears on zero
                string id = newNeighbor.Id;

                // Remove from lists if closed
                newNeighbor.ZeroEvent(s =>
                {
                    //DisconnectedEvent?.Invoke(this, newNeighbor);
                    if (Neighbors.TryRemove(id, out var _))
                        _logger.Debug($"Removed neighbor Id = {id}");
                    else
                        _logger.Fatal($"Neighbor {id} not found!");

                    return Task.CompletedTask;
                });

                // Add new neighbor
                if (!Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                {
#pragma warning disable 4014
                    newNeighbor.Zero(this);
#pragma warning restore 4014
                    _logger.Warn($"Neighbor `{ioNetClient.ListeningAddress}' already connected. Possible spoof investigate!");
                }

                //New peer connection event
                //ConnectedEvent?.Invoke(this, newNeighbor);

                //Start the source consumer on the neighbor scheduler
                try
                {
                    NeighborTasks.Add(newNeighbor.SpawnProcessingAsync());

                    //prune finished tasks
                    var remainTasks = NeighborTasks.Where(t => !t.IsCompleted).ToList();
                    NeighborTasks.Clear();
                    remainTasks.ForEach(NeighborTasks.Add);
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
            var connectedAtLeastOnce = false;

            while (!Zeroed() && !connectedAtLeastOnce)
            {
                
                var newClient = await _netServer.ConnectAsync(address).ConfigureAwait(false);
                if (newClient?.IsOperational??false)
                {
                    IoNeighbor<TJob> newNeighbor = null;
                    var neighbor = newNeighbor = MallocNeighbor(this, newClient, extraData);
                    newNeighbor = neighbor;
                    
                    //TODO does this make sense?
                    if (Neighbors.TryGetValue(newNeighbor.Id, out var zombieNeighbor))
#pragma warning disable 4014
                        zombieNeighbor.Zero(this);
#pragma warning restore 4014

                    //We capture a local varable here as newNeighbor.Id dissapears on zero
                    var id = newNeighbor.Id;

                    if (Neighbors.TryAdd(newNeighbor.Id, newNeighbor))
                    {
                        //Is this a race condition? Between subbing and being zeroed out?
                        newNeighbor.ZeroEvent(s =>
                        {
                            if (!Neighbors.TryRemove(id, out _))
                            {
                                _logger.Fatal($"Neighbor metadata expected for key `{id}'");
                            }
                            else
                            {
                                _logger.Debug($"Dropped neigbor {id}");
                            }

                            return Task.CompletedTask;
                        });

                        //TODO
                        neighbor.parm_producer_start_retry_time = 60000;
                        neighbor.parm_consumer_wait_for_producer_timeout = 60000;


                        _logger.Info($"Added {newNeighbor.Id}");

                        //ConnectedEvent?.Invoke(this, newNeighbor);

                        return newNeighbor;
                    }
                    else //strange case
                    {
                        _logger.Fatal($"Neighbor with id = {newNeighbor.Id} already exists! Closing connection...");
#pragma warning disable 4014
                        newNeighbor.Zero(this);
#pragma warning restore 4014
                    }

                    connectedAtLeastOnce = true;
                }
                else
                {
                    _logger.Error($"Failed to connect to: {address}, {address.ValidationErrorString}");
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
                _listenerTask = SpawnListenerAsync();
                await _listenerTask.ContinueWith(_=> _logger.Info($"You will be assimilated! - {ToString()} ({_.Status})")).ConfigureAwait(false);

                _logger.Info($"{GetType().Name}: Resistance is futile, {(_listenerTask.GetAwaiter().IsCompleted ? "clean" : "dirty")} exit ({_listenerTask.Status})");
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unimatrix Zero returned {ToString()}");
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        protected override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            Neighbors = null;
            NeighborTasks = null;
            _netServer = null;
            _address = null;
            _whiteList = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        protected override void ZeroManaged()
        {
            Neighbors.ToList().ForEach(kv=>kv.Value.Zero(this));
            Neighbors.Clear();

            try
            {
                _listenerTask?.Wait();
                Task.WaitAll(NeighborTasks.ToArray());
            }
            catch
            {
                // ignored
            }

            base.ZeroManaged();
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

                Neighbors[address.ToString()].Zero(this);
                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
