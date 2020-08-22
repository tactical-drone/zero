using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.cocoon.models.services;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon
{
    public class IoCcPeer : IoNeighbor<IoCcGossipMessage>
    {
        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="node">The node this peer belongs to </param>
        /// <param name="neighbor">Optional neighbor association</param>
        /// <param name="ioNetClient">The peer transport carrier</param>
        public IoCcPeer(IoNode<IoCcGossipMessage> node, IoCcNeighbor neighbor,
            IoNetClient<IoCcGossipMessage> ioNetClient) 
            : base(node, ioNetClient, userData => new IoCcGossipMessage("gossip rx", $"{ioNetClient.AddressString}", ioNetClient))
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;
            Neighbor = neighbor;

            if(Neighbor != null)
                AttachNeighbor(Neighbor);


            //Testing
            var rand = new Random((int) DateTimeOffset.Now.Ticks);
            Task.Run(async () =>
            {
                return;
                
                await Task.Delay(rand.Next(120000) + 60000, Spinners.Token).ContinueWith(r =>
                //await Task.Delay(rand.Next(60000), Spinners.Token).ContinueWith(r =>
                {
                    if(r.IsCompletedSuccessfully)
                        _logger.Fatal($"Testing SOCKET FAILURE {Id}");
                    Close();
                });
            });
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The attached neighbor
        /// </summary>
        public IoCcNeighbor Neighbor { get; private set; }

        /// <summary>
        /// CcId
        /// </summary>
        public override string Id => Neighbor?.Id??"null";

        /// <summary>
        /// Helper
        /// </summary>
        protected IoNetClient<IoCcGossipMessage> IoNetClient;

        /// <summary>
        /// Attaches a neighbor to this peer
        /// </summary>
        /// <param name="neighbor"></param>
        public void AttachNeighbor(IoCcNeighbor neighbor)
        {
            Neighbor = neighbor ?? throw new ArgumentNullException($"{nameof(neighbor)}");

            //Attach the other way
            Neighbor.AttachPeer(this);

            //peer transport closed
            IoNetClient.ClosedEvent((sender, args) => Close());
        }

        /// <summary>
        /// Closed event
        /// </summary>
        protected override void OnClosedEvent()
        {
            base.OnClosedEvent();

            Neighbor?.DetachPeer();
            Neighbor = null;
        }
    }
}
