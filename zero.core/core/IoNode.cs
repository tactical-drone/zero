using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Text;
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
    where TJob:IIoWorker    
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoNode(IoNodeAddress address, Func<IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead)
        {
            _address = address;
            _mallocNeighbor = mallocNeighbor;
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
        async Task SpawnListenerAsync()
        {
            if (_netServer != null)
                throw new ConstraintException("The network has already been started");

            _netServer = IoNetServer<TJob>.GetKindFromUrl(_address, _spinners.Token, parm_tcp_readahead);

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
                    Neighbors.TryRemove(((IoNeighbor<TJob>)s).PrimaryProducer.Key, out var _);
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
                    Task.Factory.StartNew(() => newNeighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Neighbor `{newNeighbor.PrimaryProducer.Description}' processing thread returned with errors:");
                }
            }, parm_tcp_readahead);
        }        

        /// <summary>
        /// Make sure a connection stays up
        /// </summary>        
        /// <param name="address">The remote node address</param>
        /// <returns>The async task</returns>
        public async Task SpawnConnectionAsync(IoNodeAddress address)
        {
            IoNeighbor<TJob> newNeighbor = null;
            bool connectedAtLeastOnce = false;

            while (!_spinners.IsCancellationRequested)
            {
                if (newNeighbor == null && !connectedAtLeastOnce)
                {
                    var newClient = await _netServer.ConnectAsync(address);

                    if (newClient != null && newClient.IsOperational)
                    {
                        var neighbor = newNeighbor = _mallocNeighbor(newClient);
                        _spinners.Token.Register(() => neighbor.Spinners.Cancel());

                        if (Neighbors.TryAdd(newNeighbor.PrimaryProducer.Key, newNeighbor))
                        {
                            neighbor.parm_producer_skipped_delay = 60000;
                            neighbor.parm_consumer_wait_for_producer_timeout = 60000;
                            try
                            {
#pragma warning disable 4014
                                //Task.Factory.StartNew(() => neighbor.SpawnProcessingAsync(_spinners.Token), _spinners.Token, TaskCreationOptions.LongRunning, _limitedNeighborThreadScheduler);
#pragma warning restore 4014
                            }
                            catch (Exception e)
                            {
                                _logger.Error(e, $"Neighbor `{newNeighbor.PrimaryProducer.Description}' processing thread returned with errors:");
                            }

                            //TODO remove this into the protocol?
                            if(newClient.IsOperational)
                            await newClient.ProduceAsync(client =>
                            {
                                ((IoNetSocket)client)?.SendAsync(Encoding.ASCII.GetBytes("0000015600"), 0,
                                     Encoding.ASCII.GetBytes("0000015600").Length);
                                return Task.FromResult(true) ;
                            });
                        }
                        else //strange case
                        {
                            newNeighbor.Close();
                            newNeighbor = null;
                        }

                        connectedAtLeastOnce = true;
                    }

                }
                else
                {
                    //TODO parm
                    await Task.Delay(6000);
                }

                //if (!newNeighbor?.PrimaryProducer?.IsOperational ?? false)
                //{
                //    newNeighbor.Close();
                //    Neighbors.TryRemove(newNeighbor.PrimaryProducer.Key, out _);
                //    newNeighbor = null;
                //    //TODO parm
                //    await Task.Delay(1000);
                //}
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
