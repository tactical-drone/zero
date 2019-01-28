using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NLog;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;

namespace zero.core.protocol
{
    /// <summary>
    /// Tangle Node type
    /// </summary>
    /// <typeparam name="TJob">Message job types</typeparam>
    public class TangleNode<TJob>:IoNode<TJob> 
        where TJob : IIoWorker
    {
        public TangleNode(IoNodeAddress address, Func<IoNetClient<TJob>, IoNeighbor<TJob>> mallocNeighbor, int tcpReadAhead) : base(address, mallocNeighbor, tcpReadAhead)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly Logger _logger;
        
        /// <summary>
        /// Start listener and connect back to any new connections
        /// </summary>
        /// <returns>Task</returns>
        protected override Task SpawnListenerAsync()
        {            
            PeerConnected += async (sender, ioNeighbor) => { await ConnectBackAsync(ioNeighbor); };
            return base.SpawnListenerAsync();
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
            var connectBackAddress = IoNodeAddress.Create(
                $"tcp://{((IoNetClient<TJob>) ioNeighbor.PrimaryProducer).RemoteAddress.HostStr}:{((IoNetClient<TJob>) ioNeighbor.PrimaryProducer).ListenerAddress.Port}");
#pragma warning disable 4014

            if (!Neighbors.ContainsKey(connectBackAddress.Key))
            {
                await SpawnConnectionAsync(connectBackAddress).ContinueWith(async newNeighbor =>
                {
                    if (newNeighbor.Status == TaskStatus.RanToCompletion)
                    {
                        if (newNeighbor.Result != null)
                        {
                            ((IoNetClient<TJob>) ioNeighbor.PrimaryProducer).Disconnected += (s, e) =>
                            {
                                newNeighbor.Result.Close();
                            };

                            if (newNeighbor.Result.PrimaryProducer.IsOperational)

                                await newNeighbor.Result.PrimaryProducer.ProduceAsync(client =>
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
