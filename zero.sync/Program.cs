using System;
using System.Threading;
using NLog;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.models;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled;
using zero.tangle;
using zero.tangle.entangled;
using zero.tangle.models;

namespace zero.sync
{
    class Program
    {
        static void Main(string[] args)
        {
            LogManager.LoadConfiguration("nlog.config");

            //Tangle("tcp://192.168.1.2:15600");
            CoCoon("tcp://0.0.0.0:14667", "udp://0.0.0.0:14627", null, "udp://192.168.88.253:14627");
        }

        private static void Tangle(string listenerAddress)
        {

            if (Entangled<string>.Optimized)
            {
                var tangleNode = new TangleNode<IoTangleMessage<byte[]>, byte[]>(IoNodeAddress.Create(listenerAddress),
                    (node, ioNetClient, extraData) =>
                        new TanglePeer<byte[]>((TangleNode<IoTangleMessage<byte[]>, byte[]>)node, ioNetClient),
                    TanglePeer<byte[]>.TcpReadAhead);

#pragma warning disable 4014
                var tangleNodeTask = tangleNode.StartAsync();

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero();
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero();
                tangleNodeTask.Wait();
            }
            else
            {
                var tangleNode = new TangleNode<IoTangleMessage<string>, string>(IoNodeAddress.Create(listenerAddress),
                    (node, ioNetClient, extraData) =>
                        new TanglePeer<string>((TangleNode<IoTangleMessage<string>, string>)node, ioNetClient),
                    TanglePeer<string>.TcpReadAhead);

                var tangleNodeTask = tangleNode.StartAsync();
#pragma warning restore 4014

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero();
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero();
                tangleNodeTask.Wait();
            }
        }

        private static void CoCoon(string gossipAddress, string peerAddress, string fpcAddress, string extAddress)
        {

            var cocoon = new IoCcNode(
                IoNodeAddress.Create(gossipAddress), 
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress), 
                TanglePeer<byte[]>.TcpReadAhead);

#pragma warning disable 4014
            var tangleNodeTask = cocoon.StartAsync();

            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => cocoon.Zero();

            Console.CancelKeyPress += (sender, args) =>
            {
                cocoon.Zero();
                args.Cancel = true;
            };

            tangleNodeTask.Wait();
        }
    }
}
