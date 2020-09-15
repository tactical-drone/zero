using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.core.network.ip;
using zero.tangle;
using zero.tangle.entangled;
using zero.tangle.models;

namespace zero.sync
{
    class Program
    {

        private static ConcurrentBag<IoCcNode> _nodes = new ConcurrentBag<IoCcNode>();
        private static volatile bool running;

        static void Main(string[] args)
        {
            LogManager.LoadConfiguration("nlog.config");
            var portOffset = 3000;

#if DEBUG
            portOffset = 0;
#endif

            var random = new Random((int)DateTime.Now.Ticks);
            //Tangle("tcp://192.168.1.2:15600");
            int total = 200;
            var maxNeighbors = 10;
            var tasks = new ConcurrentBag<Task<IoCcNode>>();
            tasks.Add(CoCoonAsync(IoCcIdentity.Generate(true), $"tcp://127.0.0.1:{14667 + portOffset}", $"udp://127.0.0.1:{14627 + portOffset}", null, $"udp://127.0.0.1:{14627 + portOffset}", new[] { $"udp://127.0.0.1:{14626 + portOffset}" }.ToList(), 0));
            tasks.Add(CoCoonAsync(IoCcIdentity.Generate(), $"tcp://127.0.0.1:{15667 + portOffset}", $"udp://127.0.0.1:{15627 + portOffset}", null, $"udp://127.0.0.1:{15627 + portOffset}", new[] { $"udp://127.0.0.1:{14627 + portOffset}" }.ToList(), 1));
            for (int i = 1; i < total; i++)
            {
                tasks.Add(CoCoonAsync(IoCcIdentity.Generate(), $"tcp://127.0.0.1:{15668 + portOffset + i}", $"udp://127.0.0.1:{15628 + portOffset + i}", null, $"udp://127.0.0.1:{15628 + portOffset + i}", Enumerable.Range(0, 16).Select(i => $"udp://127.0.0.1:{15628 + portOffset + random.Next(total - 1)/* % (total/6 + 1)*/}").ToList(), i));
                if(tasks.Count % 10 == 0)
                    Console.WriteLine($"Spawned {tasks.Count}/{total}...");
            }

            var task = Task.Run(() =>
            {
                Console.WriteLine("Starting autopeering...");
                var c = 0;
                foreach (var task in tasks)
                {
                    task.Start();
                    c++;
                    if (c % 20 == 0)
                    {
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Thread.Sleep(1000);
                    }
                }

                Console.WriteLine("Starting accounting...");
                c = 0;
                foreach (var task in tasks)
                {
                    task.Result.Boot();
                    c++;
                    if (c % 20 == 0)
                    {
                        Console.WriteLine($"Testing {c}/{total}...");
                        Thread.Sleep(1000);
                    }
                }
            });

            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                Console.WriteLine("###");
            };

            Console.CancelKeyPress += (sender, args) =>
            {
                Zero(total);
                args.Cancel = true;
            };

            running = true;
            var outBound = 0;
            var inBound = 0;
            var available = 0;
            var logger = LogManager.GetCurrentClassLogger();
            var reportingTask = Task.Factory.StartNew(() =>
            {
                var ooutBound = 0;
                var oinBound = 0;
                var oavailable = 0;
                long uptime = 0;
                long uptimeCount = 1;
                long opeers = 0;
                long peers = 0;
                long lastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                while (running)
                {
                    ooutBound = 0;
                    oinBound = 0;
                    oavailable = 0;
                    uptime = 0;
                    uptimeCount = 1;
                    opeers = 0;
                    foreach (var ioCcNode in _nodes)
                    {
                        opeers += ioCcNode.Neighbors.Count;
                        ooutBound += ioCcNode.OutboundCount;
                        oinBound += ioCcNode.InboundCount;
                        oavailable += ioCcNode.DiscoveryService.Neighbors.Count;
                        if (ioCcNode.DiscoveryService.Neighbors.Count > 0)
                            uptime += (long)(ioCcNode.DiscoveryService.Neighbors.Values.Select(n =>
                            {
                                if (((IoCcNeighbor)n).PeerUptime > 0)
                                {
                                    uptimeCount++;
                                    return DateTimeOffset.UtcNow.ToUnixTimeSeconds() - ((IoCcNeighbor)n).PeerUptime;
                                }
                                return 0;
                            }).Average());
                    }


                    if (outBound != ooutBound || inBound != oinBound || available != oavailable || opeers != peers)
                    {
                        var oldTotal = outBound + inBound;
                        var oldPeers = opeers;
                        outBound = ooutBound;
                        inBound = oinBound;
                        peers = opeers;
                        available = oavailable;
                        
                        ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                        ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                        ThreadPool.GetMinThreads(out var minwt, out var mincpt);

                        Console.ForegroundColor = oldPeers <= peers ? ConsoleColor.Green : ConsoleColor.Red;
                        Console.WriteLine($"out = {outBound}, int = {inBound}, available = {available}, total = {inBound + outBound}, peers = {peers}/{_nodes.Count * maxNeighbors}, {(peers) / ((double)_nodes.Count * maxNeighbors) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt})");
                        Console.ResetColor();
                    }

                    if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - lastUpdate > 120)
                    {
                        ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                        ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                        ThreadPool.GetMinThreads(out var minwt, out var mincpt);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"out = {outBound}, int = {inBound}, available = {available}, total = {inBound + outBound}, peers = {peers}/{(double)_nodes.Count * maxNeighbors}, {(peers) / (double)(_nodes.Count * maxNeighbors) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, workers = {-wt + maxwt}, ports = {-cpt + maxcpt}");
                        Console.ResetColor();
                        lastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    }

                    Thread.Sleep(1000);
                }
            }, TaskCreationOptions.LongRunning);

            Console.ReadLine();

            running = false;
            //_nodes.ToList().ForEach(n => Task.Run(() => n.ZeroAsync(null)));
            //_nodes.Clear();


            Console.ReadLine();
            Console.WriteLine($"z = {_nodes.Count(n => n.Zeroed())}/{total}");
            _nodes.Clear();
            _nodes = null;

            try
            {
                if (reportingTask.Status == TaskStatus.Running)
                    reportingTask.Dispose();
            }
            catch { }

            reportingTask = null;
            tasks.Clear();
            tasks = null;

            GC.Collect(GC.MaxGeneration);

            Console.WriteLine("##");

            Console.ReadLine();

            //var c1 = CoCoonAsync(IoCcIdentity.Generate(true), "tcp://127.0.0.1:14667", "udp://127.0.0.1:14627", null, "udp://127.0.0.1:14627", "udp://127.0.0.1:15627");
            //var c2 = CoCoonAsync(IoCcIdentity.Generate(), "tcp://127.0.0.1:15667", "udp://127.0.0.1:15627", null, "udp://127.0.0.1:15627", "udp://127.0.0.1:14627");
            //var c2 = Task.CompletedTask;
            //c1.Wait();
            //c2.Wait();
            //CoCoonAsync(IoCcIdentity.Generate(true),"tcp://127.0.0.1:14667", "udp://127.0.0.1:14627", null, "udp://127.0.0.1:14627", "udp://127.0.0.1:14626").GetAwaiter().GetResult();
            //CoCoonAsync(IoCcIdentity.Generate(), "tcp://127.0.0.1:15667", "udp://127.0.0.1:15627", null, "udp://127.0.0.1:15627", "udp://127.0.0.1:14627").GetAwaiter().GetResult();
        }

        private static void Zero(int total)
        {
            running = false;
            Console.WriteLine("#");
            SemaphoreSlim s = new SemaphoreSlim(10);
            int zeroed = 0;
            var sw = Stopwatch.StartNew();

            _nodes.ToList().ForEach(n =>
            {
                s.Wait();
                var task = Task.Run(() =>
                {
                    n.ZeroAsync(null).ConfigureAwait(false);
                    Interlocked.Increment(ref zeroed);
                    s.Release();
                });

                if (zeroed > 0 && zeroed % 5 == 0)
                {
                    Console.WriteLine(
                        $"Estimated {TimeSpan.FromSeconds((_nodes.Count - zeroed) * (zeroed * 1000 / (sw.ElapsedMilliseconds + 1)))}, zeroed = {zeroed}/{_nodes.Count}");
                }
            });

            Console.WriteLine($"z = {_nodes.Count(n => n.Zeroed())}/{total}");
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

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.ZeroAsync(null).ConfigureAwait(false);
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.ZeroAsync(null).ConfigureAwait(false);
                tangleNodeTask.Wait();
            }
            else
            {
                var tangleNode = new TangleNode<IoTangleMessage<string>, string>(IoNodeAddress.Create(listenerAddress),
                    (node, ioNetClient, extraData) =>
                        new TanglePeer<string>((TangleNode<IoTangleMessage<string>, string>)node, ioNetClient),
                    TanglePeer<string>.TcpReadAhead);

                var tangleNodeTask = tangleNode.StartAsync();


#pragma warning disable 4014
                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.ZeroAsync(null).ConfigureAwait(false);
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.ZeroAsync(null).ConfigureAwait(false);
#pragma warning restore 4014
                tangleNodeTask.Wait();
            }
        }

        private static Task<IoCcNode> CoCoonAsync(IoCcIdentity ioCcIdentity, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, List<string> bootStrapAddress, int total)
        {

            var cocoon = new IoCcNode(ioCcIdentity,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                2);

            _nodes.Add(cocoon);

#pragma warning disable 4014
            //var tangleNodeTask = Task.Factory.StartNew(async () =>
            //{
            //    await Task.Delay(total * 2);
            //    await cocoon.StartAsync();
            //}, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

            //AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            //{
            //    Console.WriteLine("=============================================================================");
            //    cocoon.ZeroAsync(null);
            //};

            //Console.CancelKeyPress += (sender, args) =>
            //{
            //    Console.WriteLine("------------------------------------------------------------------------------");
            //    cocoon.ZeroAsync(null);
            //    args.Cancel = true;
            //};

            //return tangleNodeTask.Unwrap();

            return new Task<IoCcNode>(() =>
            {
                cocoon.StartAsync().ConfigureAwait(false);
                return cocoon;
            }, TaskCreationOptions.None);
        }
    }
}
