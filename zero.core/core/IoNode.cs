using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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
            NeighborTasks = new ConcurrentDictionary<string, Task>(concurrencyLevel, new KeyValuePair<string, Task>[]{}, null!);

            //start the listener
            //_netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, ZeroConcurrencyLevel);

            //IoZeroScheduler.Zero.LoadAsyncContext(static async @this =>
            //{
            //    await ((IoNode<TJob>)@this)._netServer.ZeroHiveAsync((IoNode<TJob>)@this).FastPath();
            //},this);
            
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
        protected int parm_zombie_connect_time_threshold_s = 2; //currently takes 2 seconds to up

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
        /// A set of all node tasks that are currently running
        /// </summary>
        protected ConcurrentDictionary<string,Task> NeighborTasks;


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
                await ioNeighbor.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();
            }

            Neighbors.Clear();

            await _netServer.DisposeAsync(this, $"{nameof(ZeroManagedAsync)}: teardown").FastPath();

            //await NeighborTasks.ZeroManagedAsync(static (neighborTask, @this) =>
            //{
            //    neighborTask.Value?.Wait(TimeSpan.FromSeconds(@this.parm_nb_teardown_timeout_s));
            //    return default;
            //}, this, zero: true).FastPath();
        }

        /// <summary>
        /// Primes for DisposeAsync
        /// </summary>
        /// <returns>The task</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ZeroPrime()
        {
            base.ZeroPrime();

            if (Neighbors != null)
            {
                foreach (var ioNeighbor in Neighbors.Values)
                    ioNeighbor.ZeroPrime();
            }
        }

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        protected virtual async ValueTask BlockOnListenerAsync<T,TBoot>(Func<IoNeighbor<TJob>, T, ValueTask<bool>> handshake = null, T context = default, Func<TBoot, ValueTask> bootFunc = null, TBoot bootData = default)
        {
#if DEBUG
            _logger.Trace($"Starting lisener, {Description}");
#endif
            ////start the listener
            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _preFetch, ZeroConcurrencyLevel);
            await _netServer.ZeroHiveAsync(this).FastPath();

            await _netServer.BlockOnListenAsync(static async (state, newSocket) =>
            {
                var (@this, listenerContext, handshake) = state;
                await @this.ZeroAsync(static async state =>
                {
                    var (@this, newSocket,listenerContext, handshake) = state;
                    IoNeighbor<TJob> n;
                    if (!await ZeroEnsureConnAsync(@this, n = @this.MallocNeighbor(@this, newSocket, null), handshake, listenerContext).FastPath())
                    {
                        @this._logger.Trace($"{nameof(ZeroEnsureConnAsync)}: Accepted connection; {n.Description}");
                    }
                    else
                    {
                        @this._logger.Trace($"{nameof(ZeroEnsureConnAsync)}: Rejected connection from {newSocket}; {n.Description}");
                    }
                }, (@this, newSocket, listenerContext, handshake));
            }, (this, context, handshake), bootFunc, bootData).FastPath();
        }

        /// <summary>
        /// Ensure the connection and perform handshake
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="this"></param>
        /// <param name="newNeighbor"></param>
        /// <param name="handshake"></param>
        /// <param name="listenerContext"></param>
        /// <returns>ValueTask</returns>
        private static async ValueTask<bool> ZeroEnsureConnAsync<T>(IoNode<TJob> @this, IoNeighbor<TJob> newNeighbor, Func<IoNeighbor<TJob>, T, ValueTask<bool>> handshake, T listenerContext)
        {
            var ioNetClient = newNeighbor?.Source;

            if ((ioNetClient?.Zeroed()??true) || (@this?.Zeroed()??true))
                return false;

            try
            {
                if (await @this.ZeroAtomicAsync(static (_, state, _) =>
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
                                    if (existingNeighbor.Zeroed() || !existingNeighbor.Source.IsOperational())
                                    {
                                        var errMsg = $"{nameof(BlockOnListenerAsync)}: Connection {newNeighbor.Key} [REPLACED], uptime = {existingNeighbor.UpTime.ElapsedUtcMs()}ms";
                                        @this._logger.Warn(errMsg);

                                        //We remove the key here or async race conditions with the listener...
                                        @this.Neighbors.Remove(existingNeighbor.Key, out _);
                                        continue;
                                    }

                                    @this._logger.Warn($"{nameof(BlockOnListenerAsync)}: {newNeighbor.IoSource.Direction} Connection {newNeighbor.Source.Key} [DROPPED], existing [OK]; {existingNeighbor}");
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
                                catch when (@this.Zeroed() || existingNeighbor.Zeroed())
                                {
                                }
                                catch (Exception e) when (!@this.Zeroed() && !existingNeighbor.Zeroed())
                                {
                                    @this._logger.Trace(e,
                                        $"existingNeighbor {existingNeighbor.Description} from {@this.Description}, had errors");
                                }

                                break;
                            }
                        }

                        return new ValueTask<bool>(success);
                    }
                    catch when (@this.Zeroed() || newNeighbor.Zeroed())
                    {
                    }
                    catch (Exception e) when (!@this.Zeroed() && !newNeighbor.Zeroed())
                    {
                        @this._logger.Error(e, $"Adding new node failed! {@this.Description}");
                    }

                    return new ValueTask<bool>(false);
                }, (@this, newNeighbor)).FastPath())
                {
                    //async accept...
                    if (!await handshake(newNeighbor, listenerContext).FastPath())
                    {
                        var msg = $"{nameof(handshake)}: {((IoNetClient<TJob>)newNeighbor?.IoSource)?.Direction} connection {ioNetClient.Key} rejected.";
                        @this._logger.Trace(msg);
                        await newNeighbor.DisposeAsync(@this, msg).FastPath();
                        return false;
                    }

                    //Start processing
                    await @this.ZeroAsync(@this.BlockOnAssimilateAsync, newNeighbor).FastPath();
                }
                else
                {
                    await newNeighbor.DisposeAsync(@this, $"Failed to add new node... {@this.Description}").FastPath();
                }
            }
            catch when(@this.Zeroed()){}
            catch (Exception e) when (!@this.Zeroed())
            {
                await newNeighbor.DisposeAsync(@this, $"{nameof(handshake)} Exception: {e.Message}").FastPath();
                @this._logger.Error(e, $"Accepting connection {ioNetClient.Key} returned with errors");
            }

            return false;
        }

        /// <summary>
        /// Assimilate neighbor
        /// </summary>
        /// <param name="newNeighbor"></param>
        protected internal async ValueTask BlockOnAssimilateAsync(IoNeighbor<TJob> newNeighbor)
        {
            try
            {
                while(!newNeighbor.Zeroed() && !Zeroed())
                    await newNeighbor.BlockOnReplicateAsync().FastPath();
            }
            catch when(!Zeroed() || newNeighbor.Zeroed()){}
            catch (Exception e) when (!Zeroed() && !newNeighbor.Zeroed())
            {
                _logger.Error(e, $"{nameof(newNeighbor.BlockOnReplicateAsync)}: [FAILED]... restarting");
            }

            if(!Zeroed() && !newNeighbor.Zeroed())
                _logger.Warn($"{nameof(newNeighbor.BlockOnReplicateAsync)}: [FAILED]... restarting");
        }

        protected ValueTask<IoNeighbor<TJob>> ConnectAsync<T>(Func<IoNeighbor<TJob>, T, ValueTask<bool>> handshake,
            T context, IoNodeAddress remoteAddress, object extraData = null, bool retry = false, int timeout = 0) =>
            ConnectAndBootAsync<T, object>(handshake, context, remoteAddress, extraData, retry, timeout);

        /// <summary>
        /// Make sure a connection and execute boot function
        /// </summary>
        /// <param name="handshake"></param>
        /// <param name="context"></param>
        /// <param name="remoteAddress">The remote node address</param>
        /// <param name="extraData">Any extra data you want to send to the neighbor constructor</param>
        /// <param name="retry">Retry on failure</param>
        /// <param name="timeout">Retry timeout in ms</param>
        /// <param name="bootFunc"></param>
        /// <param name="bootData"></param>
        /// <returns>The async task</returns>
        protected async ValueTask<IoNeighbor<TJob>> ConnectAndBootAsync<T, TBoot>(
            Func<IoNeighbor<TJob>, T, ValueTask<bool>> handshake, T context, IoNodeAddress remoteAddress,
            object extraData = null, bool retry = false, int timeout = 0,
            Func<TBoot, ValueTask> bootFunc = null,
            TBoot bootData = default)
        {
            try
            {
                IoNetClient<TJob> newClient;
                IoNeighbor<TJob> newAdjunct = null;
                if ((newClient = await _netServer.ConnectAsync(remoteAddress, timeout: timeout).FastPath()) != null &&
                    !await ZeroEnsureConnAsync(this, newAdjunct = MallocNeighbor(this, newClient, extraData), handshake, context).FastPath())
                {
                    return null;
                }

                return newAdjunct;
            }
            catch when (Zeroed()){}
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e,$"{nameof(ConnectAsync)}:");
            }

            return null;
        }

        private int _activated;
        /// <summary>
        /// Start the node
        /// </summary>
        public virtual async ValueTask StartAsync<TBoot>(Func<TBoot,ValueTask> bootFunc = null, TBoot bootData = default, TaskScheduler customScheduler = null)
        {
            await ZeroAsync(static async state =>
            {
                var (@this, bootFunc, bootData) = state;
                
                if (Interlocked.CompareExchange(ref @this._activated, 1, 0) != 0)
                    return;

                await @this.BlockOnListenerAsync<object,TBoot>(bootFunc: bootFunc, bootData: bootData).FastPath();
                
                Interlocked.Exchange(ref @this._activated, 0);
            },(this, bootFunc, bootData), TaskCreationOptions.DenyChildAttach, customScheduler??IoZeroScheduler.ZeroDefault, true).FastPath();
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


                await Neighbors[address.ToString()].DisposeAsync(this, "blacklisted").FastPath();

                Neighbors.TryRemove(address.ToString(), out var ioNeighbor);
                return ioNeighbor;
            }

            _logger.Warn($"Unable to blacklist `{address}', not found!");
            return null;
        }
    }
}
