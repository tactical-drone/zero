using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.data.market;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;

namespace zero.core.core
{
    /// <summary>
    /// A p2p node
    /// </summary>
    public class IoNode<TJob> : IoNanoprobe
    where TJob : IIoJob
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> mallocNeighbor, int prefetch, int concurrencyLevel, int zeroAtomicConcurrency) : base($"{nameof(IoNode<TJob>)}", zeroAtomicConcurrency)
        {
            _address = address;
            MallocNeighbor = mallocNeighbor;
            _preFetch = prefetch;
            _concurrencyLevel = concurrencyLevel;
            _zeroAtomicConcurrency = zeroAtomicConcurrency;
            _logger = LogManager.GetCurrentClassLogger();
            var q = IoMarketDataClient.Quality;//prime market data            
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// The listening address of this node
        /// </summary>
        private IoNodeAddress _address;

        /// <summary>
        /// The listening address of this node
        /// </summary>
        public IoNodeAddress Address => _address;

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
        protected int parm_max_neighbor_pc_threads = 1;

        /// <summary>
        /// Threads per neighbor
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_zombie_connect_time_threshold = 5;


        /// <summary>
        /// expected number of waiters on this node's <see cref="IoNanoprobe.ZeroAtomicAsync"/>
        /// </summary>
        private readonly int _zeroAtomicConcurrency;

        /// <summary>
        /// TCP read ahead
        /// </summary>
        private readonly int _preFetch;

        /// <summary>
        /// TCP read ahead
        /// </summary>
        private int _concurrencyLevel;


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
        protected virtual async Task SpawnListenerAsync(Func<IoNeighbor<TJob>, Task<bool>> acceptConnection = null, Func<Task> bootstrapAsync = null)
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            //_netServer = ZeroOnCascade(IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, _concurrencyLevel), true).target;
            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, _concurrencyLevel);
            _netServer.ZeroOnCascade(this);

            await _netServer.ListenAsync(async ioNetClient =>
            {
                if (ioNetClient == null)
                    return;

                var newNeighbor = MallocNeighbor(this, ioNetClient, null);

                //superclass specific mutations
                try
                {
                    if (acceptConnection != null && !await acceptConnection.Invoke(newNeighbor).ConfigureAwait(false))
                    {
                        _logger.Trace($"Incoming connection from {ioNetClient.Key} rejected.");
                        await newNeighbor.ZeroAsync(this).ConfigureAwait(false);

                        return;
                    }
                }
                catch (Exception e)
                {
                    await newNeighbor.ZeroAsync(this).ConfigureAwait(false);
                    _logger.Error(e, $"Accepting connection {ioNetClient.Key} returned with errors");
                    return;
                }

                if (await ZeroAtomicAsync(async (s,u, d) =>
                {
                    try
                    {
                        // Does this neighbor already exist?
                        if (!Neighbors.TryAdd(newNeighbor.Key, newNeighbor))
                        {
                            //Drop incoming //TODO? Drop existing? No because of race.
                            if (Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor))
                            {
                                //Only drop incoming if the existing one is working
                                if (existingNeighbor.Source.IsOperational)
                                {
                                    _logger.Trace($"Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                    return false;
                                }
                                else//else drop existing
                                {
                                    _logger.Debug($"Connection {newNeighbor.Key} [REPLACED], existing {existingNeighbor.Key} [DC]");
                                    await existingNeighbor.ZeroAsync(new IoNanoprobe("Replaced, source dead!")).ConfigureAwait(false);
                                }
                            }
                        }

                        ZeroOnCascade(newNeighbor); 

                        //Add new neighbor
                        return await newNeighbor.ZeroAtomicAsync((s2, u2, d2) =>
                        {
                            //We use this locally captured variable as newNeighbor.Id disappears on zero
                            var id = newNeighbor.Key;
                            // Remove from lists if closed
                            var sub = newNeighbor.ZeroSubscribe(async @base =>
                            {
                                //DisconnectedEvent?.Invoke(this, newNeighbor);
                                try
                                {
                                    if (Neighbors.TryRemove(id, out var zeroNeighbor))
                                    {
                                        await zeroNeighbor.ZeroAsync(this).ConfigureAwait(false);
                                        _logger.Trace($"Removed {zeroNeighbor?.Description}");
                                    }
                                    else
                                    {
                                        _logger.Trace($"Cannot remove neighbor {id} not found!");
                                    }
                                }
                                catch (NullReferenceException e)
                                {
                                    _logger.Trace(e, Description);
                                }
                                catch (Exception e)
                                {
                                    _logger.Trace(e, $"Removing {newNeighbor.Description} from {Description}");
                                }
                            });
                            return ValueTask.FromResult(true);
                        }).ConfigureAwait(false);
                    }
                    catch (NullReferenceException) { return false; }
                    catch (TaskCanceledException) { return false; }
                    catch (OperationCanceledException) { return false; }
                    catch (ObjectDisposedException) { return false; }
                }).ConfigureAwait(false))
                {
                    //New peer connection event
                    //ConnectedEvent?.Invoke(this, newNeighbor);

                    //Start the source consumer on the neighbor scheduler
                    Assimilate(newNeighbor);
                }
                else
                {
                    await newNeighbor.ZeroAsync(this).ConfigureAwait(false);
                }
            }, bootstrapAsync).ConfigureAwait(false);
        }

        /// <summary>
        /// Assimilate neighbor
        /// </summary>
        /// <param name="newNeighbor"></param>
        public void Assimilate(IoNeighbor<TJob> newNeighbor)
        {
            try
            {
                
                //DefaultScheduler.Instance.AsLongRunning(newNeighbor.AssimilateAsync());

                var assimilation = Task.Factory.StartNew(async () =>
                {
                    await newNeighbor.AssimilateAsync();
                }, AsyncTasks.Token, TaskCreationOptions.AttachedToParent, TaskScheduler.Current);

                NeighborTasks.Add(assimilation);

                //prune finished tasks
                var remainTasks = NeighborTasks.Where(t => !t.IsCompleted).ToList();
                NeighborTasks.Clear();
                remainTasks.ForEach(NeighborTasks.Add);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Neighbor `{newNeighbor.Source.Description}' processing thread returned with errors:");
            }
        }

        /// <summary>
        /// Make sure a connection stays up
        /// </summary>        
        /// <param name="remoteAddress">The remote node address</param>
        /// <param name="extraData">Any extra data you want to send to the neighbor constructor</param>
        /// <param name="retry">Retry on failure</param>
        /// <param name="retryTimeoutMs">Retry timeout in ms</param>
        /// <returns>The async task</returns>
        public async Task<IoNeighbor<TJob>> ConnectAsync(IoNodeAddress remoteAddress, object extraData = null, bool retry = false, int retryTimeoutMs = 10000)
        {
            var newClient = await _netServer.ConnectAsync(remoteAddress).ConfigureAwait(false);

            if (newClient != null)
            {
                var newNeighbor = MallocNeighbor(this, newClient, extraData);

                //We capture a local variable here as newNeighbor.Id disappears on zero
                var id = newNeighbor.Key;

                //if (Neighbors.TryGetValue(newNeighbor.Key, out var staleNeighbor))
                //{
                //    if (staleNeighbor.Uptime.Elapsed() > parm_zombie_connect_time_threshold)
                //    {
                //        if (Neighbors.TryRemove(staleNeighbor.Key, out _))
                //        {
                //            await staleNeighbor.ZeroAsync(this).ConfigureAwait(false);
                //            _logger.Warn($"Neighbor with id = {newNeighbor.Key} already exists! Replacing connection...");
                //        }
                //        else
                //        {
                //            return null;
                //        }
                //    }
                //}

                async ValueTask<bool> OwnershipAction(IIoNanite z, object userData, bool b)
                {
                    //New neighbor?
                    if (Neighbors.TryAdd(newNeighbor.Key, newNeighbor))
                    {
                        ZeroOnCascade(newNeighbor);
                        return true;
                    }

                    //Existing and not broken neighbor?
                    if(Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor) && existingNeighbor.Uptime > parm_zombie_connect_time_threshold && existingNeighbor.Source.IsOperational)
                    {
                        return false;
                    }
                    
                    //Existing broken neighbor...
                    if (existingNeighbor != null) await existingNeighbor.ZeroAsync(this).ConfigureAwait(false);
                    return true;
                }

                if (await ZeroAtomicAsync(OwnershipAction).ConfigureAwait(false))
                {
                    newNeighbor.ZeroSubscribe(async s =>
                    {
                        try
                        {
                            IoNeighbor<TJob> closedNeighbor = null;
                            _logger.Trace(!(Neighbors?.TryRemove(id, out closedNeighbor) ?? true)
                                ? $"Neighbor metadata expected for key `{id}'"
                                : $"Dropped {closedNeighbor?.Description} from {Description}");

                            if (closedNeighbor != null)
                                await closedNeighbor.ZeroAsync(this).ConfigureAwait(false);
                        }
                        catch (NullReferenceException e)
                        {
                            _logger.Trace(e, Description);
                        }
                        catch (Exception e)
                        {
                            _logger.Fatal(e, $"Failed to remove {newNeighbor.Description} from {Description}");
                        }
                    });

                    //TODO
                    newNeighbor.parm_producer_start_retry_time = 60000;
                    newNeighbor.parm_consumer_wait_for_producer_timeout = 60000;

                    //ConnectedEvent?.Invoke(this, newNeighbor);

                    return newNeighbor;
                }
                else
                {
                    _logger.Debug($"Neighbor with id = {newNeighbor.Key} already exists! Closing connection from {newClient.IoNetSocket.RemoteNodeAddress} ...");
                    await newNeighbor.ZeroAsync(this).ConfigureAwait(false);
                }
            }

            return null;
        }

        /// <summary>
        /// Start the node
        /// </summary>
        public async Task StartAsync(Func<Task> bootstrapFunc = null)
        {
            _logger.Trace($"Unimatrix Zero: {Description}");
            try
            {
                _listenerTask = SpawnListenerAsync(bootstrapAsync: bootstrapFunc);

                var nodeTask = _listenerTask;
                await nodeTask.ConfigureAwait(false);

                _logger.Trace($"You will be assimilated! Resistance is futile, {(_listenerTask.GetAwaiter().IsCompleted ? "clean" : "dirty")} exit ({_listenerTask.Status}): {Description}");
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unimatrix Failed");
            }
        }

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            _logger = null;
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
        public override async ValueTask ZeroManagedAsync()
        {
            await _netServer.ZeroAsync(this).ConfigureAwait(false);

            foreach (var ioNeighbor in Neighbors.Values)
                await ioNeighbor.ZeroAsync(this).ConfigureAwait(false);

            Neighbors.Clear();

            try
            {
                //_listenerTask?.GetAwaiter().GetResult();
                foreach (var neighborTask in NeighborTasks)
                {
                    neighborTask.Dispose();
                }
                await Task.WhenAll(NeighborTasks).ConfigureAwait(false); //TODO teardown
            }
            catch
            {
                // ignored
            }
            NeighborTasks.Clear();

            await base.ZeroManagedAsync().ConfigureAwait(false);
            _logger.Info($"- {Description}");
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
        public async Task<IoNeighbor<TJob>> BlackListAsync(IoNodeAddress address)
        {
            if (_whiteList.TryRemove(address.ToString(), out var ioNodeAddress))
            {
                var keys = new List<string>();
                Neighbors.Values.Where(n => n.Source.Key.Contains(address.ProtocolDesc)).ToList().ForEach(n =>
                  {
                      keys.Add(n.Source.Key);
                  });


                await Neighbors[address.ToString()].ZeroAsync(this).ConfigureAwait(false);

                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
