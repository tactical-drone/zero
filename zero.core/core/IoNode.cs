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
using zero.core.patterns.schedulers;

namespace zero.core.core
{
    /// <summary>
    /// A p2p node
    /// </summary>
    public class IoNode<TPeer> : IoConfigurable
    where TPeer:IoNeighbor
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoNode(Func<IoNetClient, TPeer> mallocNeighbor)
        {
            _mallocNeighbor = mallocNeighbor;
            _limitedNeighborThreadScheduler = new LimitedThreadScheduler(parm_max_neighbor_pc_threads);
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Used to allocate peers when connections are made
        /// </summary>
        private readonly Func<IoNetClient, TPeer> _mallocNeighbor;

        /// <summary>
        /// The wrapper for <see cref="IoNetServer"/>
        /// </summary>
        private IoNetServer _netServer;

        /// <summary>
        /// All the neighbors connected to this node
        /// </summary>
        private readonly ConcurrentDictionary<string, IoNeighbor> _neighbors = new ConcurrentDictionary<string, IoNeighbor>();

        /// <summary>
        /// Used to cancel downstream proccesses
        /// </summary>
        private readonly CancellationTokenSource _spinners = new CancellationTokenSource();

        /// <summary>
        /// The sheduler used to process messages from all neighbors 
        /// </summary>
        private readonly LimitedThreadScheduler _limitedNeighborThreadScheduler;

        /// <summary>
        /// The TCP listen address
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected string parm_tcp_listen_address = "tcp://192.168.1.2"; //TODO move this into tanglepeer or zero.sync?

        /// <summary>
        /// The TCP listen port number
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected UInt16 parm_tcp_listen_port = 15600;

        /// <summary>
        /// The TCP listen address
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected string parm_tcp_connect_address = "tcp://unimatrix.uksouth.cloudapp.azure.com";

        /// <summary>
        /// The TCP listen port number
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected UInt16 parm_tcp_connect_port = 15600;

        /// <summary>
        /// The UDP listen address
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected string parm_udp_listen_address = "udp://192.168.1.2";

        /// <summary>
        /// The UDP listen port number
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected UInt16 parm_udp_listen_port = 14600;

        /// <summary>
        /// The UDP listen address
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected string parm_udp_connect_address = "udp://unimatrix.uksouth.cloudapp.azure.com";

        /// <summary>
        /// The UDP listen port number
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected UInt16 parm_udp_connect_port = 14600;

        /// <summary>
        /// The UDP listen port number
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        protected int parm_max_neighbor_pc_threads = 1;

        /// <summary>
        /// Starts the network layer
        /// </summary>
        async Task SpawnNetworkAsync(string localIp, int localPort, string remoteIp, int remotePort)
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = new IoNetServer(localIp, localPort, _spinners.Token);

#pragma warning disable 4014
            KeepOnListeningAsync();
#pragma warning restore 4014

            await StayConnectedAsync(remoteIp, remotePort);
        }

        /// <summary>
        /// Start the listener
        /// </summary>
        /// <returns>The listener task</returns>
        private async Task KeepOnListeningAsync()
        {
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
                    _neighbors.TryRemove(((IoNeighbor) s).IoNetClient.Address, out var _);
                };

                // Add new neighbor
                if (!_neighbors.TryAdd(remoteClient.Address, newNeighbor))
                {
                    newNeighbor.Close();
                    _logger.Warn( $"Neighbor `{remoteClient.Address}' already connected. Possible spoof investigate!");
                }

                //Start the producer consumer on the neighbor scheduler
                try
                {
                    Task.Factory.StartNew(() => newNeighbor.SpawnProcessingAsync(_spinners.Token),_spinners.Token, TaskCreationOptions.LongRunning, _limitedNeighborThreadScheduler);                    
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Neighbor `{newNeighbor.IoNetClient.Address}' processing thread returned with errors:");
                }
            });
        }

        /// <summary>
        /// Make sure a connection stays up
        /// </summary>
        /// <param name="remoteIp">The remote ip to connect to</param>
        /// <param name="remotePort">The remote port to connect to</param>
        /// <returns>The async task</returns>
        private async Task StayConnectedAsync(string remoteIp, int remotePort)
        {
            IoNeighbor newNeighbor = null;

            //Connect to test server         
            while (!_spinners.IsCancellationRequested)
            {
                if (newNeighbor == null)
                {
                    var newClient = await _netServer.ConnectAsync(remoteIp, remotePort);

                    if (newClient.Connected)
                    {
                        var neighbor = newNeighbor = _mallocNeighbor(newClient);
                        _spinners.Token.Register(() => neighbor.Spinners.Cancel());

                        if (_neighbors.TryAdd(newNeighbor.IoNetClient.Address, newNeighbor))
                        {
                            try
                            {
#pragma warning disable 4014
                                Task.Factory.StartNew(() => neighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, _limitedNeighborThreadScheduler);
#pragma warning restore 4014
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"Neighbor `{newNeighbor.IoNetClient.Address}' processing thread returned with errors:");
                            }
                            
                            //TODO remove this into the protocol?
                            await newClient.Execute(client =>
                            {
                                client?.SendAsync(Encoding.ASCII.GetBytes("0000015600"), 0,
                                    Encoding.ASCII.GetBytes("0000015600").Length);
                                return Task.FromResult(true);
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

                if (!newNeighbor?.IoNetClient?.Connected ?? false)
                {
                    newNeighbor.Close();
                    _neighbors.TryRemove(newNeighbor.IoNetClient.Address, out _);
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
                SpawnNetworkAsync(parm_tcp_listen_address, parm_tcp_listen_port, parm_tcp_connect_address, parm_tcp_connect_port).ContinueWith(_=> _logger.Info("You will be assimilated!"));
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
