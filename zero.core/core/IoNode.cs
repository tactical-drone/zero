using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;

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
        public IoNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> mallocNeighbor, int prefetch, int concurrencyLevel, int maxNeighbors) : base($"{nameof(IoNode<TJob>)}", concurrencyLevel)
        {
            _address = address;
            MallocNeighbor = mallocNeighbor;
            _preFetch = prefetch;
            _logger = LogManager.GetCurrentClassLogger();
            NeighborTasks = new IoQueue<Task>($"{nameof(NeighborTasks)}", maxNeighbors, concurrencyLevel);
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
        public ConcurrentDictionary<string, IoNeighbor<TJob>> Neighbors { get; protected set; } = new();

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
        protected int parm_zombie_connect_time_threshold_s = 4; //currently takes 2 seconds to up

        /// <summary>
        /// TCP read ahead
        /// </summary>
        private readonly int _preFetch;

        /// <summary>
        /// The listener task
        /// </summary>
        private ValueTask _listenerTask;

        /// <summary>
        /// A set of all node tasks that are currently running
        /// </summary>
        protected IoQueue<Task> NeighborTasks;

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async ValueTask SpawnListenerAsync<T>(Func<IoNeighbor<TJob>, T, ValueTask<bool>> acceptConnection = null, T nanite = default, Func<ValueTask> bootstrapAsync = null)
        {
            //clear previous attempts
            if (_netServer != null)
            {
                _netServer.Zero(this);
                _netServer = null;
                return;
            }
            
            //start the listener
            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, ZeroConcurrencyLevel());
            await _netServer.ZeroHiveAsync(this).FastPath().ConfigureAwait(Zc);

            _listenerTask = _netServer.ListenAsync(static async (state, ioNetClient) =>
            {
                var (@this, nanite, acceptConnection) = state;
                if (ioNetClient == null)
                    return;

                var newNeighbor = @this.MallocNeighbor(@this, ioNetClient, null);

                //superclass specific mutations
                Task acceptTask = null;
                try
                {
                    if (acceptConnection != null)
                    {
                        //async accept...
                        acceptTask = @this.ZeroAsync(  async state =>
                        {
                            var (@this, newNeighbor, acceptConnection, nanite, ioNetClient) = state;
                            if (!await acceptConnection(newNeighbor, nanite).FastPath().ConfigureAwait(@this.Zc))
                            {
                                @this._logger.Trace($"Incoming connection from {ioNetClient.Key} rejected.");
                                newNeighbor.Zero(new IoNanoprobe($"Incoming connection from {ioNetClient.Key} not accepted"));
                            }
                        }, (@this, newNeighbor, acceptConnection, nanite, ioNetClient), TaskCreationOptions.DenyChildAttach, unwrap:true).AsTask();
                    }
                    
                }
                catch (Exception e)
                {
                    newNeighbor.Zero(new IoNanoprobe($"{nameof(acceptConnection)} Exception: {e.Message}"));

                    @this._logger.Error(e, $"Accepting connection {ioNetClient.Key} returned with errors");
                    return;
                }

                if (acceptTask != null)
                {
                    await acceptTask.ContinueWith(static async (task,s) =>
                    {
                        var (@this, newNeighbor) = (ValueTuple<IoNode<TJob>, IoNeighbor<TJob>>)s;
                        if (task.IsFaulted || task.IsCanceled)
                        {
                            LogManager.GetCurrentClassLogger().Error(task.Exception);
                        }

                        if (task.IsCompletedSuccessfully)
                        {
                            if (@this.ZeroAtomic(static (_, state, _) =>
                            {
                                var (@this, newNeighbor) = state;
                                try
                                {
                                    var success = false;
                                    // Does this neighbor already exist?
                                    while (!(success = @this.Neighbors.TryAdd(newNeighbor.Key, newNeighbor)))
                                    {
                                        //Drop incoming //TODO? Drop existing? No because of race.
                                        if (@this.Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor))
                                        {
                                            try
                                            {
                                                if (!existingNeighbor.Source.IsOperational && existingNeighbor.Uptime.ElapsedMsToSec() > @this.parm_zombie_connect_time_threshold_s)
                                                {
                                                    var errMsg = $"{nameof(SpawnListenerAsync)}: Connection {newNeighbor.Key} [REPLACED], existing {existingNeighbor.Key} with uptime {existingNeighbor.Uptime.ElapsedMs()}ms [DC]";
                                                    @this._logger.Warn(errMsg);

                                                    //We remove the key here or async race conditions with the listener...
                                                    @this.Neighbors.Remove(existingNeighbor.Key, out _);
                                                    existingNeighbor.Zero(new IoNanoprobe(errMsg));
                                                    continue;
                                                }

                                                @this._logger.Warn($"{nameof(SpawnListenerAsync)}: Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                                return new ValueTask<bool>(false);

                                                ////Only drop incoming if the existing one is working and originating
                                                //if (existingNeighbor.Source.IsOriginating && existingNeighbor.Source.IsOperational)
                                                //{
                                                //    @this._logger.Warn($"Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                                //    return new ValueTask<bool>(false);
                                                //}

                                                //else  //else drop existing
                                                //{
                                                //    @this._logger.Warn($"New Connection {newNeighbor.Key} [DROPPED], [DC]");
                                                //    return new ValueTask<bool>(false);
                                                //}
                                            }
                                            catch when (@this.Zeroed() || existingNeighbor.Zeroed()) { }
                                            catch (Exception e) when (!@this.Zeroed() && !existingNeighbor.Zeroed())
                                            {
                                                @this._logger.Trace(e, $"existingNeighbor {existingNeighbor.Description} from {@this.Description}, had errors");
                                            }

                                            break;
                                        }
                                    }

                                    return new ValueTask<bool>(success);
                                }
                                catch when (@this.Zeroed() || newNeighbor.Zeroed()) { }
                                catch (Exception e) when (!@this.Zeroed() && !newNeighbor.Zeroed())
                                {
                                    @this._logger.Error(e, $"Adding new node failed! {@this.Description}");
                                }

                                return new ValueTask<bool>(false);
                            }
                        , ValueTuple.Create(@this, newNeighbor)))
                            {
                                //Start processing
                                await @this.ZeroAsync(static async state =>
                                {
                                    var (@this, newNeighbor) = state;
                                    await @this.BlockOnAssimilateAsync(newNeighbor).FastPath().ConfigureAwait(@this.Zc);
                                }, ValueTuple.Create(@this, newNeighbor), TaskCreationOptions.DenyChildAttach).FastPath()
                                .ConfigureAwait(@this.Zc);
                            }
                            else
                            {
                                newNeighbor.Zero(new IoNanoprobe("Failed to add new node..."));
                            }
                        }
                    }, (@this, newNeighbor)).ConfigureAwait(@this.Zc);
                }
            }, ValueTuple.Create(this, nanite, acceptConnection), bootstrapAsync);

            await _listenerTask.FastPath().ConfigureAwait(Zc);
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

                IoQueue<Task>.IoZNode node  = default;
                    
                node = await NeighborTasks.EnqueueAsync(ZeroOptionAsync(static async state =>
                    {
                        var (@this, newNeighbor, cfgAwait) = state;

                        try
                        {
                            while(!newNeighbor.Zeroed())
                                await newNeighbor.BlockOnReplicateAsync().FastPath().ConfigureAwait(cfgAwait);
                        }
                        catch (Exception e)
                        {
                            @this._logger.Error(e, $"{nameof(newNeighbor.BlockOnReplicateAsync)}: [FAILED]... restarting...");
                        }

                    }, ValueTuple.Create(this, newNeighbor, Zc), TaskCreationOptions.None).AsTask()).FastPath().ConfigureAwait(Zc);

                await node.Value.ContinueWith(static async (_, state) =>
                {
                    var (@this, node) = (ValueTuple<IoNode<TJob>, IoQueue<Task>.IoZNode>)state;
                    await @this.NeighborTasks.RemoveAsync(node).FastPath().ConfigureAwait(@this.Zc);

                }, (this, node));
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
            IoNetClient<TJob> newClient = null;
            IoNeighbor<TJob> newNeighbor = null;
            try
            {
                //drop connects on listener races
                if (Neighbors.ContainsKey(remoteAddress.Key))
                    return null;

                newClient = await _netServer.ConnectAsync(remoteAddress, timeout: timeout).FastPath()
                    .ConfigureAwait(Zc);

                if (newClient != null)
                {
                    //drop connects on listener races
                    if (Neighbors.ContainsKey(remoteAddress.Key))
                        return null;

                    newNeighbor = MallocNeighbor(this, newClient, extraData);

                    if (ZeroAtomic(static (_, state, _) =>
                        {
                            var (@this, newNeighbor) = state;

                            static bool AddOrUpdate(IoNode<TJob> @this, IoNeighbor<TJob> newNeighbor)
                            {
                                //New neighbor?
                                if (@this.Neighbors.TryAdd(newNeighbor.Key, newNeighbor))
                                {
                                    return true;
                                }

                                //Existing and not broken neighbor?
                                if (@this.Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor) &&
                                    existingNeighbor.Uptime.ElapsedMsToSec() > @this.parm_zombie_connect_time_threshold_s &&
                                    (existingNeighbor.Source?.IsOperational ?? false))
                                {
                                    @this._logger.Warn($"{nameof(ConnectAsync)}: Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                    return false;
                                }

                                if (existingNeighbor == null)
                                    return true;

                                var warnMsg =
                                    $"{nameof(ConnectAsync)}: Connection {newNeighbor.Key} [REPLACED], existing {existingNeighbor.Key} with uptime {existingNeighbor.Uptime.ElapsedMs()}ms [DC]";
                                @this._logger.Warn(warnMsg);

                                //Existing broken neighbor...
                                existingNeighbor.Zero(new IoNanoprobe(warnMsg));

                                @this.Neighbors.TryRemove(newNeighbor.Key, out _);

                                return AddOrUpdate(@this, newNeighbor);
                            }

                            return new ValueTask<bool>(AddOrUpdate(@this, newNeighbor));
                        }, ValueTuple.Create(this, newNeighbor)))
                    {
                        return newNeighbor;
                    }
                    else
                    {
                        _logger.Debug($"Neighbor with id = {newNeighbor.Key} already exists! Closing connection from {newClient.IoNetSocket.RemoteNodeAddress} ...");
                        newNeighbor.Zero(new IoNanoprobe("Dropped, connection already exists"));
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Error(e,$"{nameof(ConnectAsync)}:");
            }
            finally
            {
                if (newClient != null && newNeighbor == null)
                {
                    newClient.Zero(new IoNanoprobe($"{nameof(newClient)} is not null but {nameof(newNeighbor)} is. Should not be..."));
                }
            }

            return null;
        }

        /// <summary>
        /// Start the node
        /// </summary>
        public async ValueTask StartAsync(Func<ValueTask> bootstrapFunc = null)
        {
            _logger.Trace($"Unimatrix ZeroAsync: {Description}");
            try
            {
                var retry = 3;
                while (!Zeroed() && retry-- > 0)
                {
                    await SpawnListenerAsync<object>(bootstrapAsync: bootstrapFunc).FastPath().ConfigureAwait(Zc);
                    if (!Zeroed())
                        _logger.Warn($"Listener restart... {Description}");
                    else
                        Zero(this);
                }

                if(!Zeroed())
                    _logger.Trace($"{Description}: {(_listenerTask.IsCompletedSuccessfully ? "clean" : "dirty")} exit ({_listenerTask}), retries left = {retry}");
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e) when (!Zeroed())
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
            NeighborTasks = default;
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
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(Zc);

            foreach (var ioNeighbor in Neighbors.Values)
                ioNeighbor.Zero(this);

            Neighbors.Clear();

            _netServer?.Zero(this);

            try
            {
                //await Task.WhenAll(NeighborTasks.Select(t=>t.Value)).ConfigureAwait(Zc); //TODO teardown
            }
            catch
            {
                // ignored
            }

            await NeighborTasks.ZeroManagedAsync(static (task, @this) =>
            {
                if(task.Status != TaskStatus.Running)
                    task.Dispose();
                return default;
            }, this, zero:true).FastPath().ConfigureAwait(Zc);
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
        public Task<IoNeighbor<TJob>> BlackListAsync(IoNodeAddress address)
        {
            if (_whiteList.TryRemove(address.ToString(), out var ioNodeAddress))
            {
                var keys = new List<string>();
                Neighbors.Values.Where(n => n.Source.Key.Contains(address.ProtocolDesc)).ToList().ForEach(n =>
                  {
                      keys.Add(n.Source.Key);
                  });


                Neighbors[address.ToString()].Zero(this);

                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return Task.FromResult(ioNeighbor);
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
