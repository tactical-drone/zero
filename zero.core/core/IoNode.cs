using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.schedulers;

namespace zero.core.core
{
    /// <summary>
    /// A p2p node
    /// </summary>
    public class IoNode : IoConfigurable        
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoNode(IoNodeAddress address, Func<IoNetClient, IoNeighbor> mallocNeighbor)
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
        private readonly Func<IoNetClient, IoNeighbor> _mallocNeighbor;

        /// <summary>
        /// The wrapper for <see cref="IoNetServer"/>
        /// </summary>
        private IoNetServer _netServer;

        /// <summary>
        /// All the neighbors connected to this node
        /// </summary>
        private readonly ConcurrentDictionary<string, IoNeighbor> _neighbors = new ConcurrentDictionary<string, IoNeighbor>();

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

            _netServer = IoNetServer.GetKindFromUrl(_address, _spinners.Token);

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
                    _neighbors.TryRemove(((IoNeighbor)s).WorkSource.AddressString, out var _);
                };

                // Add new neighbor
                if (!_neighbors.TryAdd(remoteClient.AddressString, newNeighbor))
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
                    _logger.Error(e, $"Neighbor `{newNeighbor.WorkSource.AddressString}' processing thread returned with errors:");
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
            IoNeighbor newNeighbor = null;

            while (!_spinners.IsCancellationRequested)
            {
                if (newNeighbor == null)
                {
                    var newClient = await _netServer.ConnectAsync(address);

                    if (newClient.IsSocketConnected())
                    {
                        var neighbor = newNeighbor = _mallocNeighbor(newClient);
                        _spinners.Token.Register(() => neighbor.Spinners.Cancel());

                        if (_neighbors.TryAdd(newNeighbor.WorkSource.AddressString, newNeighbor))
                        {
                            try
                            {
#pragma warning disable 4014
                                Task.Factory.StartNew(() => neighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, _limitedNeighborThreadScheduler);
#pragma warning restore 4014
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"Neighbor `{newNeighbor.WorkSource.AddressString}' processing thread returned with errors:");
                            }

                            //TODO remove this into the protocol?
                            if(newClient.IsSocketConnected())
                            await newClient.Execute(client =>
                            {
                                client?.SendAsync(Encoding.ASCII.GetBytes("0000015600"), 0,
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

                if (!newNeighbor?.WorkSource?.IsSocketConnected() ?? false)
                {
                    newNeighbor.Close();
                    _neighbors.TryRemove(newNeighbor.WorkSource.AddressString, out _);
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
            _neighbors.ToList().ForEach(n => n.Value.Close());
            _neighbors.Clear();
            _logger.Info("Resistance is futile");
        }
    }
}
