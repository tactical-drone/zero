using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.conf;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.misc;
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
        public TangleNode(IoNodeAddress address, Func<IoNode<TJob>, IoNetClient<TJob>, object, IoNeighbor<TJob>> mallocNeighbor, int tcpPrefetch) : base(address, mallocNeighbor, tcpPrefetch, 1)
        {
            _logger = LogManager.GetCurrentClassLogger();            
            Milestones = new Milestone<TKey>(AsyncTasks.Token);
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
        protected override async ValueTask SpawnListenerAsync<T>(Func<IoNeighbor<TJob>, T, ValueTask<bool>> connectionReceivedAction = null, T nanite = default, Func<ValueTask> bootstrapAsync = null)
        {
            //ConnectedEvent += async (sender, ioNeighbor) => { await ConnectBackAsync(ioNeighbor); };

            await base.SpawnListenerAsync(static async (ioNeighbor,@this) =>
            {
                await @this.ConnectBackAsync(ioNeighbor);

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
            },this, bootstrapAsync);
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
            var connectBackAddress = IoNodeAddress.Create($"{((IoNetClient<TJob>) ioNeighbor.Source).IoNetSocket.RemoteNodeAddress}");
#pragma warning disable 4014
            
            if (!Neighbors.ContainsKey(connectBackAddress.Key))
            {
                var newNeighbor = await ConnectAsync(connectBackAddress).ConfigureAwait(false);
                if (newNeighbor!= null)
                {
                    await ((IoNetClient<TJob>) ioNeighbor.Source).ZeroSubAsync<object>(async (_,_) => await newNeighbor.ZeroAsync(this).ConfigureAwait(false)).FastPath().ConfigureAwait(false);

                    if (newNeighbor.Source.IsOperational)
                        await newNeighbor.Source.ProduceAsync<object>((client,_, _, _) =>
                        {
                            //TODO
                            // await ((IoNetSocket) client)?.SendAsync(Encoding.ASCII.GetBytes("0000015600"), 0,
                            //     Encoding.ASCII.GetBytes("0000015600").Length).FastPath().ConfigureAwait(false);
                            return new ValueTask<bool>(true);
                        }).FastPath().ConfigureAwait(false);
                }
                else
                {
                    _logger.Error($"Unable to connect back to `{connectBackAddress}'");
                }
            }
            else
            {

            }
#pragma warning restore 4014
        }
    }
}
