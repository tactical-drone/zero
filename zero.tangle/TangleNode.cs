using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.tangle.data.redis.configurations.tangle;
using zero.tangle.utils;

namespace zero.tangle
{
    /// <summary>
    /// Tangle Node type
    /// </summary>
    /// <typeparam name="TJob">Message job types</typeparam>
    /// <typeparam name="TKey">Type of the key field</typeparam>
    public class TangleNode<TJob,TKey>:IoNode<TJob> 
        where TJob : IIoWorker
    {        
        public TangleNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead) : base(address, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();            
            Milestones = new Milestone<TKey>(Spinners.Token);
        }

        private readonly Logger _logger;

        private readonly CancellationTokenSource Spinners = new CancellationTokenSource();

        /// <summary>
        /// The latest milestone seen
        /// </summary>
        public IIoTransactionModel<TKey> LatestMilestoneTransaction { get; set; }

        public Milestone<TKey> Milestones { get; protected set; }

        [IoParameter]
        public string parm_coo_address = "KPWCHICGJZXKE9GSUDXZYUAPLHAKAHYHDXNPHENTERYMMBQOPSQIDENXKLKCEYCPVTZQLEEJVYJZV9BWU";

        /// <summary>
        /// Start listener and connect back to any new connections
        /// </summary>
        /// <returns>Task</returns>
        protected override async Task SpawnListenerAsync(Action<IoNeighbor<TJob>> connectionReceivedAction = null)
        {            
            //PeerConnected += async (sender, ioNeighbor) => { await ConnectBackAsync(ioNeighbor); };

            await base.SpawnListenerAsync(async ioNeighbor =>
            {
                await ConnectBackAsync(ioNeighbor);

                var connectTask = IoTangleTransactionHashCache.Default();
                await connectTask.ContinueWith(r =>
                {
                    switch (r.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            break;
                        case TaskStatus.RanToCompletion:
                            ioNeighbor.Producer.RecentlyProcessed = r.Result;
                            break;
                    }
                });
            });
        }

        /// <summary>
        /// Connect back to a iota peer
        /// </summary>
        /// <param name="ioNeighbor">The neighbor that connected to us</param>
        /// <returns>Task</returns>
        private async Task ConnectBackAsync(IoNeighbor<TJob> ioNeighbor)
        {            
            //TangleNode<TJob> node = (TangleNode<TJob>) sender;
            //TODO fix
            var connectBackAddress = IoNodeAddress.Create($"tcp://{((IoNetClient<TJob>) ioNeighbor.Producer).RemoteAddress.HostStr}:{((IoNetClient<TJob>) ioNeighbor.Producer).ListenerAddress.Port}");
#pragma warning disable 4014
            
            if (!Neighbors.ContainsKey(connectBackAddress.Key))
            {
                await SpawnConnectionAsync(connectBackAddress).ContinueWith(async newNeighbor =>
                {
                    if (newNeighbor.Status == TaskStatus.RanToCompletion)
                    {
                        if (newNeighbor.Result != null)
                        {
                            ((IoNetClient<TJob>) ioNeighbor.Producer).Disconnected += (s, e) =>
                            {
                                newNeighbor.Result.Close();
                            };

                            if (newNeighbor.Result.Producer.IsOperational)

                                await newNeighbor.Result.Producer.ProduceAsync(client =>
                                {
                                    //TODO
                                    ((IoNetSocket) client)?.SendAsync(Encoding.ASCII.GetBytes("0000015600"), 0,
                                        Encoding.ASCII.GetBytes("0000015600").Length);
                                    return Task.FromResult(true);
                                });
                        }
                        else
                        {
                            _logger.Error($"Unable to connect back to `{connectBackAddress}'");
                        }
                    }
                    else
                    {
                        _logger.Error(newNeighbor.Exception,
                            $"Connect back to neighbor `{connectBackAddress}' returned with errors:");
                    }
                });
            }
            else
            {
            }
#pragma warning restore 4014
        }
    }
}
