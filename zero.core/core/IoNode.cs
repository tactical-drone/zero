using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.data.market;
using zero.core.misc;
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
        public IoNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> mallocNeighbor, int prefetch, int concurrencyLevel) : base($"{nameof(IoNode<TJob>)}", concurrencyLevel)
        {
            _address = address;
            MallocNeighbor = mallocNeighbor;
            _preFetch = prefetch;
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
        public ConcurrentDictionary<string, IoNeighbor<TJob>> Neighbors = new();

        /// <summary>
        /// Allowed clients
        /// </summary>
        private ConcurrentDictionary<string, IoNodeAddress> _whiteList = new();

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
        /// TCP read ahead
        /// </summary>
        private readonly int _preFetch;

        /// <summary>
        /// 
        /// </summary>
        private ValueTask _listenerTask;

        /// <summary>
        /// A set of all node tasks that are currently running
        /// </summary>
        protected ConcurrentBag<Task> NeighborTasks = new();

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async ValueTask SpawnListenerAsync<T>(Func<IoNeighbor<TJob>, T, ValueTask<bool>> acceptConnection = null, T nanite = default, Func<ValueTask> bootstrapAsync = null)
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, ZeroConcurrencyLevel());
            await _netServer.ZeroHiveAsync(this).FastPath().ConfigureAwait(Zc);

            await _netServer.ListenAsync(static async (state, ioNetClient) =>
            {
                var (@this,nanite, acceptConnection) = state;
                if (ioNetClient == null)
                    return;

                var newNeighbor = @this.MallocNeighbor(@this, ioNetClient, null);

                //superclass specific mutations
                try
                {
                    if (acceptConnection != null && !await acceptConnection(newNeighbor, nanite).FastPath().ConfigureAwait(@this.Zc)) //TODO ?
                    {
                        @this._logger.Trace($"Incoming connection from {ioNetClient.Key} rejected.");
                        await newNeighbor.ZeroAsync(@this).FastPath().ConfigureAwait(@this.Zc);
                        return;
                    }
                }
                catch (Exception e)
                {
                    await newNeighbor.ZeroAsync(@this).ConfigureAwait(@this.Zc);
                    @this._logger.Error(e, $"Accepting connection {ioNetClient.Key} returned with errors");
                    return;
                }

                if (await @this.ZeroAtomicAsync(static async (_,state, _) =>
                {
                    var (@this, newNeighbor) = state;
                    try
                    {
                        // Does this neighbor already exist?
                        if (!@this.Neighbors.TryAdd(newNeighbor.Key, newNeighbor))
                        {
                            //Drop incoming //TODO? Drop existing? No because of race.
                            if (@this.Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor))
                            {
                                //Only drop incoming if the existing one is working
                                if (existingNeighbor.Source.IsOperational)
                                {
                                    @this._logger.Trace(
                                        $"Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                    return false;
                                }
                                else //else drop existing
                                {
                                    @this._logger.Debug(
                                        $"Connection {newNeighbor.Key} [REPLACED], existing {existingNeighbor.Key} [DC]");
                                    await existingNeighbor.ZeroAsync(new IoNanoprobe("Replaced, source dead!"))
                                        .FastPath().ConfigureAwait(@this.Zc);
                                }
                            }
                        }

                        //ZeroOnCascade(newNeighbor); 

                        //Add new neighbor
                        return await newNeighbor.ZeroAtomicAsync(static async (_, state, _) =>
                        {
                            var (@this, newNeighbor) = state;
                            //We use this locally captured variable as newNeighbor.Id disappears on zero
                            var id = newNeighbor.Key;
                            // Remove from lists if closed
                            await newNeighbor.ZeroSubAsync(static async (from, state) =>
                            {
                                var (@this, id, newNeighbor) = state;
                                //DisconnectedEvent?.Invoke(this, newNeighbor);
                                try
                                {
                                    if (@this.Neighbors.TryRemove(id, out var zeroNeighbor))
                                    {
                                        await zeroNeighbor.ZeroAsync(@this).ConfigureAwait(@this.Zc);
                                        @this._logger.Trace($"Removed {zeroNeighbor?.Description}");
                                    }
                                    else
                                    {
                                        @this._logger.Trace($"Cannot remove neighbor {id} not found!");
                                    }

                                    return true;
                                }
                                catch (NullReferenceException e)
                                {
                                    @this._logger?.Trace(e, @this.Description);
                                }
                                catch (Exception e)
                                {
                                    @this._logger?.Trace(e,
                                        $"Removing {newNeighbor.Description} from {@this.Description}");
                                }

                                return false;
                            }, ValueTuple.Create(@this, id, newNeighbor)).FastPath().ConfigureAwait(@this.Zc);
                            return true;
                        }, ValueTuple.Create(@this, newNeighbor)).FastPath().ConfigureAwait(@this.Zc);
                    }
                    catch when (@this.Zeroed())
                    {
                    }
                    catch (Exception e)when (!@this.Zeroed())
                    {
                        @this._logger.Error(e, $"Adding new node failed! {@this.Description}");
                    }
                    return false;
                },ValueTuple.Create(@this, newNeighbor)).ConfigureAwait(@this.Zc))
                {
                    //Start processing
                    await @this.ZeroAsync(static async state =>
                    {
                        var (@this, newNeighbor) = state;
                        await @this.BlockOnAssimilateAsync(newNeighbor).FastPath().ConfigureAwait(@this.Zc);
                    }, ValueTuple.Create(@this, newNeighbor), TaskCreationOptions.DenyChildAttach).FastPath().ConfigureAwait(false);
                }
                else
                {
                    await newNeighbor.ZeroAsync(@this).ConfigureAwait(@this.Zc);
                }
            }, ValueTuple.Create(this,nanite,acceptConnection),bootstrapAsync).FastPath().ConfigureAwait(Zc);
        }

        /// <summary>
        /// Assimilate neighbor
        /// </summary>
        /// <param name="newNeighbor"></param>
        public virtual async ValueTask BlockOnAssimilateAsync(IoNeighbor<TJob> newNeighbor)
        {
            try
            {
                //Start replication
                await ZeroOptionAsync(static async state =>
                {
                    var (newNeighbor, cfgAwait) = state;
                    await newNeighbor.BlockOnReplicateAsync().ConfigureAwait(cfgAwait);
                }, ValueTuple.Create(newNeighbor, Zc), TaskCreationOptions.None).FastPath().ConfigureAwait(Zc);
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
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
        /// <param name="timeout">Retry timeout in ms</param>
        /// <returns>The async task</returns>
        public async ValueTask<IoNeighbor<TJob>> ConnectAsync(IoNodeAddress remoteAddress, object extraData = null,
            bool retry = false, int timeout = 0)
        {
            var newClient = await _netServer.ConnectAsync(remoteAddress, timeout: timeout).FastPath().ConfigureAwait(Zc);

            if (newClient != null)
            {
                var newNeighbor = MallocNeighbor(this, newClient, extraData);

                //We capture a local variable here as newNeighbor.Id disappears on zero
                var id = newNeighbor.Key;
                
                if (await ZeroAtomicAsync(static async (_,state,_) =>
                {
                    var (@this, newNeighbor) = state;
                    //New neighbor?
                    if (@this.Neighbors.TryAdd(newNeighbor.Key, newNeighbor))
                    {
                        //ZeroOnCascade(newNeighbor);
                        return true;
                    }

                    //Existing and not broken neighbor?
                    if(@this.Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor) && existingNeighbor.Uptime.ElapsedMs() > @this.parm_zombie_connect_time_threshold && existingNeighbor.Source.IsOperational)
                    {
                        return false;
                    }
                    
                    //Existing broken neighbor...
                    if (existingNeighbor != null) await existingNeighbor.ZeroAsync(@this).ConfigureAwait(@this.Zc);
                    return true;
                }, ValueTuple.Create(this,newNeighbor)).ConfigureAwait(Zc))
                {
                    await newNeighbor.ZeroSubAsync(static async (from, state ) =>
                    {
                        var (@this, id, newNeighbor) = state;
                        try
                        {
                            IoNeighbor<TJob> closedNeighbor = null;
                            @this._logger.Trace(!(@this.Neighbors?.TryRemove(id, out closedNeighbor) ?? true)
                                ? $"Neighbor metadata expected for key `{id}'"
                                : $"Dropped {closedNeighbor?.Description} from {@this.Description}");

                            if (closedNeighbor != null)
                                await closedNeighbor.ZeroAsync(@this).ConfigureAwait(@this.Zc);

                            return true;
                        }
                        catch (NullReferenceException e)
                        {
                            @this._logger?.Trace(e, @this.Description);
                        }
                        catch (Exception e)
                        {
                            @this._logger.Fatal(e, $"Failed to remove {newNeighbor.Description} from {@this.Description}");
                        }

                        return false;
                    }, ValueTuple.Create(this, id, newNeighbor)).FastPath().ConfigureAwait(Zc);

                    //TODO
                    newNeighbor.parm_producer_start_retry_time = 60000;
                    newNeighbor.parm_consumer_wait_for_producer_timeout = 60000;

                    //ConnectedEvent?.Invoke(this, newNeighbor);

                    return newNeighbor;
                }
                else
                {
                    _logger.Debug($"Neighbor with id = {newNeighbor.Key} already exists! Closing connection from {newClient.IoNetSocket.RemoteNodeAddress} ...");
                    await newNeighbor.ZeroAsync(this).ConfigureAwait(Zc);
                }
            }

            return null;
        }

        /// <summary>
        /// Start the node
        /// </summary>
        public async ValueTask StartAsync(Func<ValueTask> bootstrapFunc = null)
        {
            _logger.Trace($"Unimatrix Zero: {Description}");
            try
            {
                while (!Zeroed())
                {
                    await SpawnListenerAsync<object>(bootstrapAsync: bootstrapFunc).FastPath().ConfigureAwait(Zc);
                    _logger.Warn($"Listener restarte.... {Description}");
                }

                _logger.Trace(
                    $"You will be assimilated! Resistance is futile, {(_listenerTask.IsCompletedSuccessfully ? "clean" : "dirty")} exit ({_listenerTask}): {Description}");
            }
            //catch (InvalidOperationException e)
            //{
            //    _logger.Trace(e, $"Unimatrix Failed ~> {Description}");
            //}
            catch (Exception e)
            {
                _logger.Error(e, $"Unimatrix Failed ~> {Description}");
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
            if(_netServer != null)
                await _netServer.ZeroAsync(this).ConfigureAwait(Zc);

            foreach (var ioNeighbor in Neighbors.Values)
                await ioNeighbor.ZeroAsync(this).ConfigureAwait(Zc);

            Neighbors.Clear();

            try
            {
                await Task.WhenAll(NeighborTasks).ConfigureAwait(Zc); //TODO teardown
            }
            catch
            {
                // ignored
            }
            NeighborTasks.Clear();

            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);
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


                await Neighbors[address.ToString()].ZeroAsync(this).ConfigureAwait(Zc);

                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
