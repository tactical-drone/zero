using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using NLog;
using zero.core.conf;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.runtime.scheduler;

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
            NeighborTasks = new IoQueue<Task>($"{nameof(NeighborTasks)}", 32, concurrencyLevel, IoQueue<Task>.Mode.DynamicSize);
        }

        /// <summary>
        /// logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// The listening address of this node
        /// </summary>
        private readonly IoNodeAddress _address;

        /// <summary>
        /// The listening address of this node
        /// </summary>
        public IoNodeAddress Address => _address;

        /// <summary>
        /// Used to allocate peers when connections are made
        /// </summary>
        public Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> MallocNeighbor { get; protected set; }

        /// <summary>
        /// The wrapper for <see cref="IoNetServer{TJob}"/>
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
        /// Threads per neighbor
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_nb_teardown_timeout_s  = 60; //currently takes 2 seconds to up

        /// <summary>
        /// Read ahead
        /// </summary>
        private readonly int _preFetch;

        /// <summary>
        /// Read ahead
        /// </summary>
        public int PreFetch => _preFetch;

        /// <summary>
        /// The listener task
        /// </summary>
        private ValueTask _listenerTask;

        /// <summary>
        /// A set of all node tasks that are currently running
        /// </summary>
        protected IoQueue<Task> NeighborTasks;


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
            _whiteList = null;
#endif
        }

        /// <summary>
        /// zero managed
        /// </summary>
        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath();

            foreach (var ioNeighbor in Neighbors.Values)
            {
                await ioNeighbor.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            }

            Neighbors.Clear();

            _netServer?.Zero(this, $"{nameof(ZeroManagedAsync)}: teardown");

            await NeighborTasks.ZeroManagedAsync(static async (neighborTask, @this) =>
            {
                await IoZeroScheduler.AsyncBridge.RunAsync(async () =>
                {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
                    await neighborTask.Value.WithTimeout(TimeSpan.FromSeconds(@this.parm_nb_teardown_timeout_s));
#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                });
            }, this, zero: true).FastPath();
        }

        /// <summary>
        /// Primes for Zero
        /// </summary>
        /// <returns>The task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ZeroPrime()
        {
            base.ZeroPrime();
            foreach (var ioNeighbor in Neighbors.Values)
                ioNeighbor.ZeroPrime();
        }

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async ValueTask SpawnListenerAsync<T>(Func<IoNeighbor<TJob>, T, ValueTask<bool>> acceptConnection = null, T context = default, Func<ValueTask> bootstrapAsync = null)
        {
            //clear previous attempts
            if (_netServer != null)
            {
                await _netServer.Zero(this, "Recycled").FastPath();
                _netServer = null;
                return;
            }

#if DEBUG
            _logger.Trace($"Starting lisener, {Description}");   
#endif
            //start the listener
            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, ZeroConcurrencyLevel());
            await _netServer.ZeroHiveAsync(this).FastPath();

            _listenerTask = _netServer.ListenAsync(static async (state, newSocket) =>
            {
                var (@this, nanite, acceptConnection) = state;
                if (newSocket == null)
                    return;

                if (@this.Neighbors.Count == @this.NeighborTasks.Capacity)
                {
                    await newSocket.Zero(@this, $"{nameof(_netServer.ListenAsync)}: neighbor count maxed out at {@this.Neighbors.Count}").FastPath();
                    return;
                }

                //if (@this.Neighbors.TryGetValue(newSocket.Key, out var currentDrone))
                //{
                //    var ioNetSource = (IoNetClient<TJob>)currentDrone.Source;
                //    var oldSocket = ioNetSource.IoNetSocket.NativeSocket;
                //    ((IoNetClient<TJob>)currentDrone.Source).IoNetSocket.NativeSocket = newSocket.IoNetSocket.NativeSocket;

                //    try
                //    {
                //        if (oldSocket.Connected)
                //            oldSocket.Close();
                //    }
                //    catch
                //    {
                //        // ignored
                //    }

                //    @this._logger.Debug($"{nameof(SpawnListenerAsync)}: Quick reconnect {@this.Description}");

                //    return;
                //}

                var newNeighbor = @this.MallocNeighbor(@this, newSocket, null);
                
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
                            if (!await acceptConnection(newNeighbor, nanite).FastPath())
                            {
                                @this._logger.Trace($"Incoming connection from {ioNetClient.Key} rejected.");
                                await newNeighbor.Zero(@this,$"Incoming connection from {ioNetClient.Key} not accepted").FastPath();
                            }
                        }, (@this, newNeighbor, acceptConnection, nanite, ioNetClient: newSocket), TaskCreationOptions.DenyChildAttach, unwrap:true).AsTask();
                    }
                    
                }
                catch (Exception e)
                {
                    await newNeighbor.Zero(@this,$"{nameof(acceptConnection)} Exception: {e.Message}").FastPath();

                    @this._logger.Error(e, $"Accepting connection {newSocket.Key} returned with errors");
                    return;
                }

                if (acceptTask != null)
                {
                    //TODO
                    await acceptTask.ContinueWith(static async (task,s) =>
                    {
                        var (@this, newNeighbor) = (ValueTuple<IoNode<TJob>, IoNeighbor<TJob>>)s;
                        if (task.IsFaulted || task.IsCanceled)
                        {
                            LogManager.GetCurrentClassLogger().Error(task.Exception);
                        }

                        if (task.IsCompletedSuccessfully)
                        {
                            if (await @this.ZeroAtomic(static async (_, state, _) => 
                                    {
                                        var (@this, newNeighbor) = state;
                                        try
                                        {
                                            bool success;
                                            // Does this neighbor already exist?
                                            while (!(success = @this.Neighbors.TryAdd(newNeighbor.Key, newNeighbor)))
                                            {
                                                //Drop incoming //TODO? Drop existing? No because of race.
                                                if (@this.Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor))
                                                {
                                                    try
                                                    {
                                                        if (!existingNeighbor.Source.IsOperational() && existingNeighbor.UpTime.ElapsedMsToSec() > @this.parm_zombie_connect_time_threshold_s)
                                                        {
                                                            var errMsg = $"{nameof(SpawnListenerAsync)}: Connection {newNeighbor.Key} [REPLACED], existing {existingNeighbor.Key} with uptime {existingNeighbor.UpTime.ElapsedMs()}ms [DC]";
                                                            @this._logger.Warn(errMsg);

                                                            //We remove the key here or async race conditions with the listener...
                                                            @this.Neighbors.Remove(existingNeighbor.Key, out _);
                                                            await existingNeighbor.Zero(@this,errMsg).FastPath();
                                                            continue;
                                                        }

                                                        @this._logger.Warn($"{nameof(SpawnListenerAsync)}: Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                                        return false;

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

                                            return success;
                                        }
                                        catch when (@this.Zeroed() || newNeighbor.Zeroed()) { }
                                        catch (Exception e) when (!@this.Zeroed() && !newNeighbor.Zeroed())
                                        {
                                            @this._logger.Error(e, $"Adding new node failed! {@this.Description}");
                                        }
                                        return false;
                                    },ValueTuple.Create(@this, newNeighbor)).FastPath())
                            {
                                //Start processing
                                await @this.BlockOnAssimilateAsync(newNeighbor).FastPath();
                            }
                            else
                            {
                                await newNeighbor.Zero(@this, $"Failed to add new node... {@this.Description}").FastPath();
                            }
                        }
                    }, (@this, newNeighbor), CancellationToken.None, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);
                }
            }, ValueTuple.Create(this, context, acceptConnection), bootstrapAsync);

            await _listenerTask.FastPath();
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

                var node = await NeighborTasks.EnqueueAsync(ZeroOptionAsync(static async state =>
                {
                    var (@this, newNeighbor) = state;

                    try
                    {
                        while(!newNeighbor.Zeroed())
                            await newNeighbor.BlockOnReplicateAsync().FastPath();
                    }
                    catch when(!@this.Zeroed() || newNeighbor.Zeroed()){}
                    catch (Exception e) when (!@this.Zeroed() && !newNeighbor.Zeroed())
                    {
                        @this._logger.Error(e, $"{nameof(newNeighbor.BlockOnReplicateAsync)}: [FAILED]... restarting...");
                    }

                    if(!@this.Zeroed() && !newNeighbor.Zeroed())
                        @this._logger.Warn($"{nameof(newNeighbor.BlockOnReplicateAsync)}: [FAILED]... restarting...");
                }, ValueTuple.Create(this, newNeighbor), TaskCreationOptions.DenyChildAttach).AsTask()).FastPath();

                await node.Value.ContinueWith(static async (_, state) =>
                {
                    var (@this, node) = (ValueTuple<IoNode<TJob>, IoQueue<Task>.IoZNode>)state;
                    await @this.NeighborTasks.RemoveAsync(node).FastPath();
                }, (this, node), CancellationToken.None, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);
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
        public async ValueTask<IoNeighbor<TJob>> ConnectAsync(IoNodeAddress remoteAddress, object extraData = null, bool retry = false, int timeout = 0)
        {
            IoNetClient<TJob> newClient = null;
            IoNeighbor<TJob> newNeighbor = null;
            try
            {
                //drop connects on listener races
                if (Neighbors.ContainsKey(remoteAddress.Key))
                    return null;

                newClient = await _netServer.ConnectAsync(remoteAddress, timeout: timeout).FastPath();

                if (newClient != null)
                {
                    //drop connects on listener races
                    if (Neighbors.ContainsKey(remoteAddress.Key))
                        return null;

                    newNeighbor = MallocNeighbor(this, newClient, extraData);

                    if (newNeighbor != null && await ZeroAtomic(static (_, state, _) =>
                        {
                            var (@this, newNeighbor) = state;

                            static async ValueTask<bool> AddOrUpdate(IoNode<TJob> @this, IoNeighbor<TJob> newNeighbor)
                            {
                                try
                                {
                                    //New neighbor?
                                    if (@this.Neighbors.TryAdd(newNeighbor.Key, newNeighbor))
                                    {
                                        return true;
                                    }

                                    //Existing and not broken neighbor?
                                    if (@this.Neighbors.TryGetValue(newNeighbor.Key, out var existingNeighbor) &&
                                        existingNeighbor.UpTime.ElapsedMsToSec() >
                                        @this.parm_zombie_connect_time_threshold_s &&
                                        existingNeighbor.Source.IsOperational())
                                    {
                                        @this._logger.Warn(
                                            $"{nameof(ConnectAsync)}: Connection {newNeighbor.Key} [DROPPED], existing {existingNeighbor.Key} [OK]");
                                        return false;
                                    }

                                    if (existingNeighbor == null)
                                        return true;

                                    var warnMsg =
                                        $"{nameof(ConnectAsync)}: Connection {newNeighbor.Key} [REPLACED], existing {existingNeighbor.Key} with uptime {existingNeighbor.UpTime.ElapsedMs()}ms [DC]";
                                    @this._logger.Warn(warnMsg);

                                    //Existing broken neighbor...
                                    await existingNeighbor.Zero(@this, warnMsg).FastPath();

                                    @this.Neighbors.TryRemove(newNeighbor.Key, out _);

                                    return await AddOrUpdate(@this, newNeighbor).FastPath();
                                }
                                catch when (@this.Zeroed() || newNeighbor.Zeroed())
                                {
                                }
                                catch (Exception e) when (!@this.Zeroed() && !newNeighbor.Zeroed())
                                {
                                    @this._logger.Error(e,$"{nameof(AddOrUpdate)}:");
                                }

                                return false;
                            }

                            return AddOrUpdate(@this, newNeighbor);
                        }, (this, newNeighbor)).FastPath())
                    {
                        return newNeighbor;
                    }
                    else if(newNeighbor != null)
                    {
                        _logger.Debug($"Neighbor with id = {newNeighbor.Key} already exists! Closing connection from {newClient.IoNetSocket.RemoteNodeAddress} ...");
                        await newNeighbor.Zero(this, "Dropped, connection already exists").FastPath();
                    }
                }
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e,$"{nameof(ConnectAsync)}:");
            }
            finally
            {
                if (newClient != null && newNeighbor == null)
                {
                    await newClient.Zero(this, $"{nameof(newClient)} is not null but {nameof(newNeighbor)} is. Should not be...").FastPath();
                }
            }

            return null;
        }

        private int _activated;
        /// <summary>
        /// Start the node
        /// </summary>
        public async ValueTask StartAsync(Func<ValueTask> bootstrapFunc = null, TaskScheduler customScheduler = null)
        {
            if(Interlocked.CompareExchange(ref _activated, 1, 0) != 0)
                return;
            
            _logger.Trace($"Unimatrix ZeroAsync: {Description}");
            
            await ZeroAsync(static async state =>
            {
                var (@this, bootstrapFunc) = state;
                var retry = 3;
                while (!@this.Zeroed() && retry-- > 0)
                {
                    await @this.SpawnListenerAsync<object>(bootstrapAsync: bootstrapFunc).FastPath();
                    if (!@this.Zeroed())
                        @this._logger.Warn($"Listener restart... {@this.Description}");
                    else
                        await @this.Zero(@this, "Zeroed").FastPath();
                }

                if (!@this.Zeroed())
                    @this._logger.Trace($"{@this.Description}: {(@this._listenerTask.IsCompletedSuccessfully ? "clean" : "dirty")} exit ({@this._listenerTask}), retries left = {retry}");
            },(this, bootstrapFunc), TaskCreationOptions.DenyChildAttach, customScheduler??IoZeroScheduler.ZeroDefault, true).FastPath();
            
            _activated = 0;
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


                await Neighbors[address.ToString()].Zero(this, "blacklisted").FastPath();

                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
