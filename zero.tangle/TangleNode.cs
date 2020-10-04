using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.tangle.data.redis.configurations.tangle;
using zero.tangle.models;
using zero.tangle.utils;

namespace zero.tangle
{
    /// <summary>
    /// Tangle Node type
    /// </summary>
    /// <typeparam name="TJob">Message job types</typeparam>
    /// <typeparam name="TKey">Type of the key field</typeparam>
    public class TangleNode<TJob,TKey>:IoNode<TJob> 
        where TJob : IIoJob
    {        
        public TangleNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> mallocNeighbor, int tcpPrefetch) : base(address, mallocNeighbor, tcpPrefetch, 2)
        {
            _logger = LogManager.GetCurrentClassLogger();            
            Milestones = new Milestone<TKey>(AsyncToken.Token);
        }

        private readonly Logger _logger;

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
        protected override async Task SpawnListenerAsync(Func<IoNeighbor<TJob>, Task<bool>> connectionReceivedAction = null, Func<Task> bootstrapAsync = null)
        {
            //ConnectedEvent += async (sender, ioNeighbor) => { await ConnectBackAsync(ioNeighbor); };

            await base.SpawnListenerAsync(async ioNeighbor =>
            {
                await ConnectBackAsync(ioNeighbor);

                var connectTask = IoTangleTransactionHashCache.DefaultAsync();
                await connectTask.ContinueWith(r =>
                {
                    switch (r.Status)
                    {
                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            break;
                        case TaskStatus.RanToCompletion:
                            ioNeighbor.Source.RecentlyProcessed = r.Result;
                            break;
                    }
                });
                return true;
            }, bootstrapAsync);
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
            var connectBackAddress = IoNodeAddress.Create($"tcp://{((IoNetClient<TJob>) ioNeighbor.Source).RemoteAddress.Ip}:{((IoNetClient<TJob>) ioNeighbor.Source).ListeningAddress.Port}");
#pragma warning disable 4014
            
            if (!Neighbors.ContainsKey(connectBackAddress.Key))
            {
                await ConnectAsync(connectBackAddress).ContinueWith(async newNeighbor =>
                {
                    if (newNeighbor.Status == TaskStatus.RanToCompletion)
                    {
                        if (newNeighbor.Result != null)
                        {
                            ((IoNetClient<TJob>) ioNeighbor.Source).ZeroEvent(async s => await newNeighbor.Result.ZeroAsync(this).ConfigureAwait(false));

                            if (newNeighbor.Result.Source.IsOperational)

                                await newNeighbor.Result.Source.ProduceAsync((client,_, __, ___) =>
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
