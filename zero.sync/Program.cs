using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
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

        private static ConcurrentBag<IoCcNode> _nodes = new ConcurrentBag<IoCcNode>();

        static void Main(string[] args)
        {
            LogManager.LoadConfiguration("nlog.config");

            //Tangle("tcp://192.168.1.2:15600");

            var tasks = new ConcurrentBag<Task>();
            tasks.Add(CoCoon(IoCcIdentity.Generate(true), "tcp://0.0.0.0:14667", "udp://0.0.0.0:14627", null, "udp://192.168.88.253:14627", "udp://192.168.88.253:14626"));
            tasks.Add(CoCoon(IoCcIdentity.Generate(), $"tcp://0.0.0.0:{15667}", $"udp://0.0.0.0:{15627}", null, "udp://192.168.88.253:15627", "udp://192.168.88.253:14627"));
            for (int i = 1; i < 200; i++)
            {
                tasks.Add(CoCoon(IoCcIdentity.Generate(), $"tcp://0.0.0.0:{15667 + i}", $"udp://0.0.0.0:{15627+i}", null, $"udp://192.168.88.253:{15627 + i}", $"udp://192.168.88.253:{15627 + i - 1}"));
            }

            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                Console.WriteLine("=============================================================================");
            };

            Console.CancelKeyPress += (sender, args) =>
                {
                    Console.WriteLine("------------------------------------------------------------------------------");
                    _nodes.ToList().ForEach(n=>Task.Run(()=>n.Zero(null)));
                    _nodes.Clear();
                    args.Cancel = true;
                };

            tasks.ToList().ForEach(t=>t.Wait());

            //var c1 = CoCoon(IoCcIdentity.Generate(true), "tcp://0.0.0.0:14667", "udp://0.0.0.0:14627", null, "udp://192.168.88.253:14627", "udp://192.168.88.253:15627");
            //var c2 = CoCoon(IoCcIdentity.Generate(), "tcp://0.0.0.0:15667", "udp://0.0.0.0:15627", null, "udp://192.168.88.253:15627", "udp://192.168.88.253:14627");
            //var c2 = Task.CompletedTask;
            //c1.Wait();
            //c2.Wait();
            //CoCoon(IoCcIdentity.Generate(true),"tcp://0.0.0.0:14667", "udp://0.0.0.0:14627", null, "udp://192.168.88.253:14627", "udp://192.168.88.253:14626").GetAwaiter().GetResult();
            //CoCoon(IoCcIdentity.Generate(), "tcp://0.0.0.0:15667", "udp://0.0.0.0:15627", null, "udp://192.168.88.253:15627", "udp://192.168.88.253:14627").GetAwaiter().GetResult();
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

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero(null);
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero(null);
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

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero(null);
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero(null);
                tangleNodeTask.Wait();
            }
        }

        private static Task CoCoon(IoCcIdentity ioCcIdentity, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, string bootStrapAddress)
        {

            var cocoon = new IoCcNode(ioCcIdentity,
                IoNodeAddress.Create(gossipAddress), 
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                IoNodeAddress.Create(bootStrapAddress),
                2);

            _nodes.Add(cocoon);

#pragma warning disable 4014
            var tangleNodeTask = Task.Factory.StartNew(async () => await cocoon.StartAsync(), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            //{
            //    Console.WriteLine("=============================================================================");
            //    cocoon.Zero(null);
            //};

            //Console.CancelKeyPress += (sender, args) =>
            //{
            //    Console.WriteLine("------------------------------------------------------------------------------");
            //    cocoon.Zero(null);
            //    args.Cancel = true;
            //};

            return tangleNodeTask.Unwrap();
        }
    }
}
