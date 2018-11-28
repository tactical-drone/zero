using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.schedulers;

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
        public IoNode(IoNodeAddress address, Func<IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor)
        {
            _address = address;
            _mallocNeighbor = mallocNeighbor;
            _limitedNeighborThreadScheduler = new LimitedThreadScheduler(parm_max_neighbor_pc_threads);
            _logger = LogManager.GetCurrentClassLogger();
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
        public readonly ConcurrentDictionary<int, IoNeighbor<TJob>> Neighbors = new ConcurrentDictionary<int, IoNeighbor<TJob>>();

        /// <summary>
        /// Used to cancel downstream processes
        /// </summary>
        private readonly CancellationTokenSource _spinners = new CancellationTokenSource();

        /// <summary>
        /// The scheduler used to process messages from all neighbors 
        /// </summary>
        private readonly LimitedThreadScheduler _limitedNeighborThreadScheduler;
               
        /// <summary>
        /// The UDP listen port number
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_max_neighbor_pc_threads = 1;

        /// <summary>
        /// Starts the node's listener
        /// </summary>
        async Task SpawnListenerAsync()
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _spinners.Token);

            await _netServer.StartListenerAsync(remoteClient =>
            {
                var newNeighbor = _mallocNeighbor(remoteClient);

                // Register close hooks
                var cancelRegistration = _spinners.Token.Register(() =>
                {
                    newNeighbor.Close();
                });

                // Remove from lists if closed
                newNeighbor.Closed += (s, e) =>
                {
                    cancelRegistration.Dispose();
                    Neighbors.TryRemove(((IoNeighbor<TJob>)s).WorkSource.Key, out var _);
                };

                // Add new neighbor
                if (!Neighbors.TryAdd(remoteClient.Key, newNeighbor))
                {
                    newNeighbor.Close();
                    _logger.Warn($"Neighbor `{remoteClient.AddressString}' already connected. Possible spoof investigate!");
                }

                //Start the producer consumer on the neighbor scheduler
                try
                {
                    Task.Factory.StartNew(() => newNeighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, _limitedNeighborThreadScheduler);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Neighbor `{newNeighbor.WorkSource.Description}' processing thread returned with errors:");
                }
            });
        }        

        /// <summary>
        /// Make sure a connection stays up
        /// </summary>        
        /// <param name="address">The remote node address</param>
        /// <returns>The async task</returns>
        public async Task SpawnConnectionAsync(IoNodeAddress address)
        {
            IoNeighbor<TJob> newNeighbor = null;

            while (!_spinners.IsCancellationRequested)
            {
                if (newNeighbor == null)
                {
                    var newClient = await _netServer.ConnectAsync(address);

                    if (newClient.IsSocketConnected())
                    {
                        var neighbor = newNeighbor = _mallocNeighbor((IoNetClient<TJob>) newClient);
                        _spinners.Token.Register(() => neighbor.Spinners.Cancel());

                        if (Neighbors.TryAdd(newNeighbor.WorkSource.Key, newNeighbor))
                        {
                            try
                            {
#pragma warning disable 4014
                                Task.Factory.StartNew(() => neighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, _limitedNeighborThreadScheduler);
#pragma warning restore 4014
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"Neighbor `{newNeighbor.WorkSource.Description}' processing thread returned with errors:");
                            }

                            //TODO remove this into the protocol?
                            if(newClient.IsSocketConnected())
                            await newClient.Produce(client =>
                            {
                                ((IoNetSocket)client)?.SendAsync(Encoding.ASCII.GetBytes("0000015600"), 0,
                                     Encoding.ASCII.GetBytes("0000015600").Length);
                                return Task.FromResult(Task.CompletedTask) ;
                            });
                        }
                        else //strange case
                        {
                            newNeighbor.Close();
                            newNeighbor = null;
                        }
                    }

                }
                else
                {
                    //TODO parm
                    await Task.Delay(6000);
                }

                if (!newNeighbor?.WorkSource?.IsOperational ?? false)
                {
                    newNeighbor.Close();
                    Neighbors.TryRemove(newNeighbor.WorkSource.Key, out _);
                    newNeighbor = null;
                    //TODO parm
                    await Task.Delay(1000);
                }
            }
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
    }
}
