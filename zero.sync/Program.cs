using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Web;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.tangle;
using zero.tangle.entangled;
using zero.tangle.models;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace zero.sync
{
    class Program
    {

        private static ConcurrentBag<CcCollective> _nodes = new ConcurrentBag<CcCollective>();
        private static volatile bool _running;
        private static volatile bool _verboseGossip;
        private static volatile bool _startAccounting;
        private static bool Zc = IoNanoprobe.ContinueOnCapturedContext;
        private static int _rampDelay = 300;
        private static int _rampTarget = 40;
        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder
                        .ConfigureKestrel(options =>
                        {
                            options.ListenAnyIP(27021,
                                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
                        })
                        .UseStartup<StartServices>()
                        .UseNLog()
                        .ConfigureLogging(builder =>
                        {
                            builder.AddFilter(level => level > LogLevel.Error);
                            builder.ClearProviders();
                            builder.SetMinimumLevel(LogLevel.Error);
                        });

                });

        /// <summary>
        /// Cluster test mode
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            //SemTest();
            //QueueTestAsync();
            //BagTest();
            //BagTest2();

            LogManager.LoadConfiguration("nlog.config");
            var portOffset = 5051;
            
            IHost host = null;
            var grpc = Task.Factory.StartNew(() =>
            {
                host = CreateHostBuilder(args).Build();
                host.Run();
            }, TaskCreationOptions.DenyChildAttach);

            var random = new Random((int)DateTime.Now.Ticks);
            //Tangle("tcp://192.168.1.2:15600");
            var total = 2000;
            var maxDrones = 8;
            var maxAdjuncts = 16;
            var tasks = new ConcurrentBag<Task<CcCollective>>
            {
                CoCoonAsync(CcDesignation.Generate(true), $"tcp://127.0.0.1:{14667}", $"udp://127.0.0.1:{1234}",
                    $"tcp://127.0.0.1:{11667}", $"udp://127.0.0.1:{1234}", new[] {$"udp://127.0.0.1:{1233}"}.ToList()),
                CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15670 + portOffset}",
                    $"udp://127.0.0.1:{1235 + portOffset}", $"tcp://127.0.0.1:{11667 + portOffset}",
                    $"udp://127.0.0.1:{1235 + portOffset}",
                    new[]
                    {
                        $"udp://127.0.0.1:{1234}"
                    }.ToList())
            };

            for (var i = 2; i < total; i++)
            {
                //if(1234 + portOffset + i == 1900 )
                //    continue;

                tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{1234 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{1234 + portOffset + i}", new[] { $"udp://127.0.0.1:{1234 + portOffset + i - 1}", $"udp://127.0.0.1:{1234 + portOffset + (i + 16) % total}", $"udp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1235 + portOffset}" }.ToList()));
                if (tasks.Count % 10 == 0)
                    Console.WriteLine($"Spawned {tasks.Count}/{total}...");
            }

            //Console.WriteLine("Prepping...");
            //Thread.Sleep(10000);

            var task = Task.Factory.StartNew(async () =>
            {
                Console.WriteLine($"Starting auto peering...  {tasks.Count}");
                var c = 1;
                var rateLimit = 9000;
                var injectionCount = 75;
                foreach (var task in tasks)
                {
                    var h = Task.Factory.StartNew(() => task.Start(), TaskCreationOptions.DenyChildAttach);
                    if (c % injectionCount == 0)
                    {
                        await Task.Delay(rateLimit += 10).ConfigureAwait(Zc);

                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        if(injectionCount > 40)
                            injectionCount--;
                    }

                    c++;
                }
            }, TaskCreationOptions.DenyChildAttach);

            _running = true;
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
                int minOut = int.MaxValue;
                int minIn = int.MaxValue;
                int minOutC = 0;
                int minInC = 0;
                int empty = 0;
                long lastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                long perfTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), totalPerfTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                long opsCheckpoint = ThreadPool.CompletedWorkItemCount, totalOpsCheckpoint = ThreadPool.CompletedWorkItemCount;
                while (_running)
                {
                    try
                    {
                        ooutBound = 0;
                        oinBound = 0;
                        oavailable = 0;
                        opeers = 0;
                        empty = 0;
                        minOut = int.MaxValue;
                        minIn = int.MaxValue;
                        minOutC = 0;
                        minInC = 0;
                        uptime = 0;
                        uptimeCount = 1;
                        foreach (var ioCcNode in _nodes)
                        {
                            try
                            {
                                opeers += ioCcNode.Drones.Count;
                                var e = ioCcNode.EgressCount;
                                var i = ioCcNode.IngressCount;
                                minOut = Math.Min(minOut, e);
                                minIn = Math.Min(minIn, i);
                                if (ioCcNode.TotalConnections == 0)
                                    empty++;
                                if (ioCcNode.EgressCount == 0)
                                    minOutC++;
                                if (ioCcNode.IngressCount == 0)
                                    minInC++;

                                if (ioCcNode.TotalConnections > ioCcNode.MaxDrones)
                                {
                                    Console.WriteLine($"[{ioCcNode.Description}]");
                                    foreach (var d in ioCcNode.Neighbors.Values)
                                    {
                                        var drone = (CcDrone) d;
                                        Console.WriteLine($"t = {ioCcNode.TotalConnections} ({ioCcNode.IngressCount},{ioCcNode.EgressCount}), [{ioCcNode.Description}] -> {drone.Description} ][ {drone.Adjunct.MetaDesc}, uptime = {drone.Uptime.ElapsedMs():0.0}s");
                                    }
                                }
                            
                                ooutBound += e;
                                oinBound += i;
                                oavailable += ioCcNode.Hub.Neighbors.Values.Count(static n => ((CcAdjunct)n).Proxy);
                            
                                uptime += ioCcNode.Hub.Neighbors.Values.Select(static n =>
                                {
                                    if (((CcAdjunct)n).IsDroneConnected && ((CcAdjunct)n).AttachTimestamp > 0)
                                        return ((CcAdjunct)n).AttachTimestamp.Elapsed();
                                
                                    return 0;
                                }).Sum();

                                uptimeCount += ioCcNode.TotalConnections;
                            }
                            catch 
                            {
                                
                            }
                        }

                        if (outBound != ooutBound || inBound != oinBound || available != oavailable || opeers != peers)
                        {
                            var prevPeers = peers;
                            outBound = ooutBound;
                            inBound = oinBound;
                            peers = opeers;
                            available = oavailable;

                            ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                            ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                            ThreadPool.GetMinThreads(out var minwt, out var mincpt);

                            var localOps = (ThreadPool.CompletedWorkItemCount - opsCheckpoint);
                            var totalOps = (ThreadPool.CompletedWorkItemCount - totalOpsCheckpoint);
                            var fps = localOps / (double)perfTime.ElapsedMsToSec();
                            var tfps = totalOps / (double)totalPerfTime.ElapsedMsToSec();
                            opsCheckpoint = ThreadPool.CompletedWorkItemCount;
                            perfTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            try
                            {
                                Console.ForegroundColor = prevPeers <= peers ? ConsoleColor.Green : ConsoleColor.Red;
                                Console.WriteLine($"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / ((double)_nodes.Count * maxDrones) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt}), con = {CcAdjunct.ConnectionTime/(CcAdjunct.ConnectionCount + 1.0):0.0}ms,  [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
                            }
                            catch{}
                            finally
                            {
                                Console.ResetColor();
                            }

                        }
                        else if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - lastUpdate > 120)
                        {
                            ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                            ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                            ThreadPool.GetMinThreads(out var minwt, out var mincpt);


                            var localOps = (ThreadPool.CompletedWorkItemCount - opsCheckpoint);
                            var totalOps = (ThreadPool.CompletedWorkItemCount - totalOpsCheckpoint);
                            var fps = localOps / (double)perfTime.ElapsedMsToSec();
                            var tfps = totalOps / (double)totalPerfTime.ElapsedMsToSec();
                            perfTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            opsCheckpoint = ThreadPool.CompletedWorkItemCount;

                            Console.ForegroundColor = ConsoleColor.Green;
                            try
                            {
                                Console.WriteLine($"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / (double)(_nodes.Count * maxDrones) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, w = {-wt + maxwt}, ports = {-cpt + maxcpt}, con = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1.0):0.0}ms, [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
                            }
                            catch
                            {
                                // ignored
                            }
                            finally
                            {
                                Console.ResetColor();
                            }
                            lastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();                            
                        }

                        if (!_startAccounting && (inBound + outBound) / (double)(_nodes.Count * maxDrones) > 0.955)
                        {
                            _startAccounting = true;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Thread.Sleep(30000);
                }
            }, TaskCreationOptions.DenyChildAttach);

           

            string line;
            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var tsOrig = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var cOrig = ThreadPool.CompletedWorkItemCount;
            var c = ThreadPool.CompletedWorkItemCount;

            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                Console.WriteLine("###");
            };

            Console.CancelKeyPress += (sender, args) =>
            {
                args.Cancel = true;
                ZeroAsync(total).AsTask().GetAwaiter();
            };

            while ((line = Console.ReadLine())!= null && !line.StartsWith("quit"))
            {
                if (line == "gc")
                {
                    var pinned = GC.GetGCMemoryInfo(GCKind.Any).PinnedObjectsCount;
                    GC.Collect(GC.MaxGeneration);
                    Console.WriteLine($"Pinned was = {pinned}, now {GC.GetGCMemoryInfo(GCKind.Any).PinnedObjectsCount}");
                }
                
                if (line == "q")
                    break;

                if (line == "t")
                {
                    Console.WriteLine($"w = {ThreadPool.ThreadCount}, p = {ThreadPool.PendingWorkItemCount}, t = {ThreadPool.CompletedWorkItemCount}, {(ThreadPool.CompletedWorkItemCount - cOrig) / (double)tsOrig.ElapsedMsToSec():0.0} ops, c = {ThreadPool.CompletedWorkItemCount-c}, {(ThreadPool.CompletedWorkItemCount-c)/(double)ts.ElapsedMsToSec():0.0} tps");
                    ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    c = ThreadPool.CompletedWorkItemCount;
                }

                if (line.StartsWith("log "))
                {
                    try
                    {
                        LogManager.Configuration.Variables["zeroLogLevel"] = $"{line.Split(' ')[1]}";
                        LogManager.ReconfigExistingLoggers();
                    }
                    catch { }
                }

                if (line.StartsWith("ramp "))
                {                    
                    try
                    {
                        if (line.Contains("target"))
                        {
                            _rampTarget = int.Parse(line.Split(' ')[2]);
                            Console.WriteLine($"rampTarget = {_rampTarget}");
                        }
                            

                        if (line.Contains("delay"))
                        {
                            _rampDelay = int.Parse(line.Split(' ')[2]);
                            Console.WriteLine($"rampDelay = {_rampDelay}");
                        }
                            
                    }
                    catch { }
                }

                var gossipTasks = new List<Task>();

                if (line.StartsWith("gossip"))
                {
                    if (line.Contains("verbose"))
                    {
                        _verboseGossip = !_verboseGossip;
                        Console.WriteLine($"gossip verbose = {_verboseGossip}");
                    }
                    else if (line.Contains("restart"))
                    {
                        if (gossipTasks.Count > 0)
                        {
                            _running = false;
                            _startAccounting = false;
                            Task.WaitAll(gossipTasks.ToArray());
                            gossipTasks.Clear();
                            AutoPeeringEventService._queuedEvents[0].ClearAsync().GetAwaiter();
                        }

                        long v = 1;
                        long C = 0;
                        _rampDelay = 1500;
                        _rampTarget = 1000;
                        var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        var threads = 2;
                        _running = true;
                        for (int i = 0; i < threads; i++)
                        {
                            var i1 = i;
                            gossipTasks.Add(Task.Factory.StartNew(async () =>
                            {
                                while (!_startAccounting)
                                    await Task.Delay(1000).ConfigureAwait(Zc);
                                Console.WriteLine($"Starting accounting... {tasks.Count}");
                                await Task.Delay(random.Next(1000 / threads) * i1).ConfigureAwait(Zc);
                                while (_running)
                                {
                                    if (!_startAccounting)
                                    {
                                        await Task.Delay(1000).ConfigureAwait(Zc);
                                        continue;
                                    }

                                    foreach (var task in tasks)
                                    {
                                        if (!_startAccounting)
                                            break;
                                        if (!await task.Result.BootAsync(Interlocked.Increment(ref v), tasks.Count).FastPath()
                                                .ConfigureAwait(Zc))
                                        {
                                            if (_verboseGossip)
                                                Console.Write("*");
                                            continue;
                                        }

                                        if (_verboseGossip)
                                            Console.Write(".");

                                        //ramp
                                        if (_rampDelay > _rampTarget)
                                        {
                                            Interlocked.Decrement(ref _rampDelay);
                                            //Console.WriteLine($"ramp = {_rampDelay}");
                                        }

                                        var d = _rampDelay;
                                        d++;
                                        await Task.Delay(d).ConfigureAwait(Zc);

                                        if (Interlocked.Increment(ref C) % 5000 == 0)
                                        {
                                            Console.WriteLine($"{C / ((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start) / 1000.0):0.0} g/ps");
                                            Interlocked.Exchange(ref C, 0);
                                            start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                        }
                                    }
                                }
                                Console.WriteLine("Stopped gossip...");
                            }, TaskCreationOptions.DenyChildAttach));
                            _startAccounting = true;
                        }
                        Console.WriteLine($"Stopped auto peering...  {tasks.Count}");
                    }
                    else
                    {
                        _startAccounting = !_startAccounting;
                        Console.WriteLine($"gossip = {_startAccounting}");
                    }
                }

                if (line.StartsWith("dn "))
                {
                    try
                    {
                        _nodes.FirstOrDefault(n => n.CcId.IdString() == line.Split(' ')[1]).PrintNeighborhood();
                    }
                    catch (Exception){}                    
                }

                if (line.StartsWith("ec"))
                {
                    if (line.Contains("clear"))
                    {
                        foreach (var ccCollective in _nodes)
                            ccCollective.ClearEventCounter();
                    }
                    else
                    {
                        try
                        {
                            long sum = 0;
                            foreach (var ccCollective in _nodes)
                                sum += ccCollective.EventCount;

                            var ave = sum / _nodes.Count;
                            long aveDelta = 0;
                            var std = 0;
                            var i = 0;
                            foreach (var ccCollective in _nodes)
                            {
                                var delta = ccCollective.EventCount - ave;
                                aveDelta += Math.Abs(delta);

                                //Console.Write($"[{i++}]({delta}), ");
                                Console.Write($"{delta}, ");
                            }

                            aveDelta /= _nodes.Count;

                            foreach (var ccCollective in _nodes)
                            {
                                var delta = ccCollective.EventCount - ave;
                                if (aveDelta > 0 && delta < 0 && ave - delta > aveDelta * 2)
                                    std++;
                            }

                            Console.WriteLine($"\nave = {ave}, std = {std}/{_nodes.Count} ({aveDelta})");
                        }
                        catch (Exception) { }
                    }
                }

                if (line.StartsWith("stream"))
                {
                    AutoPeeringEventService.ToggleActive();
                    Console.WriteLine($"event stream = {(AutoPeeringEventService.Operational? "On":"Off")}");
                }

                if (line.StartsWith("zero"))
                {
                    ZeroAsync(total).AsTask().GetAwaiter();
                }
            };
            Console.WriteLine($"z = {_nodes.Count(n => n.Zeroed())}/{total}");
            

            _running = false;
            
            try
            {                
                if (reportingTask.Status == TaskStatus.Running)
                    reportingTask.Dispose();
                if (task.Status == TaskStatus.Running)
                    task.Dispose();
            }
            catch { }

            reportingTask = null;
            tasks.Clear();
            tasks = null;
            _nodes.Clear();
            _nodes = null;

            var s = host.StopAsync();
            LogManager.Shutdown();

            Console.WriteLine("## - done");
            
            GC.Collect(GC.MaxGeneration);

            Console.ReadLine();
        }

        /// <summary>
        /// Tests queue
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private static Task QueueTestAsync() //TODO make unit tests
        {

            IoQueue<int> q = new IoQueue<int>("test", 2000000000, 100);
            var head = q.PushBackAsync(2).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();
            q.PushBackAsync(1).FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.EnqueueAsync(3).FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.EnqueueAsync(4).FastPath().ConfigureAwait(Zc).GetAwaiter();
            var five = q.PushBackAsync(5).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();
            q.EnqueueAsync(6).FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.EnqueueAsync(7).FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.EnqueueAsync(8).FastPath().ConfigureAwait(Zc).GetAwaiter();
            var tail = q.EnqueueAsync(9).FastPath().ConfigureAwait(Zc).GetAwaiter().GetResult();

            Console.WriteLine("Init");
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }
            Console.WriteLine("\nDQ mid");
            q.RemoveAsync(five).FastPath().ConfigureAwait(Zc).GetAwaiter();
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            var c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Prev;
            }
            
            Console.WriteLine("\nDQ head");
            q.RemoveAsync(q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Prev;
            }
            
            Console.WriteLine("\nDQ tail");
            q.RemoveAsync(q.Tail).FastPath().ConfigureAwait(Zc).GetAwaiter();
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Prev;
            }
            

            Console.WriteLine("\nDQ second last prime");
            q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.RemoveAsync(q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.RemoveAsync(q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
            q.DequeueAsync().FastPath().ConfigureAwait(Zc).GetAwaiter();

            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Prev;
            }
            
            Console.WriteLine("\nDQ second last");

            if (true)
            {
                q.RemoveAsync(q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();
                q.RemoveAsync(q.Head).FastPath().ConfigureAwait(Zc).GetAwaiter();    
            }

            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Head;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Next;
            }

            var _concurrentTasks = new List<Task>();

            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var rounds = 20;
            var mult = 1000000;
            for (var i = 0; i < rounds; i++)
            {
                Console.Write(".");
                var i3 = i;
                _concurrentTasks.Add(Task.Factory.StartNew(async () =>
                {                    
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {
                            var eq1 = q.PushBackAsync(i3);
                            var eq2 = q.EnqueueAsync(i3 + 1);
                            var i1 = q.PushBackAsync(i3 + 2);
                            var i2 = q.EnqueueAsync(i3 + 3);
                            var i4 = q.PushBackAsync(i3 + 4);

                            await eq2;
                            await eq1;
                            await i4;
                            await i2;
                            await i1;

                            //await q.RemoveAsync(i1.Result);

                            //await q.RemoveAsync(i2.Result);
                            var d2 = q.DequeueAsync();
                            
                            var d4 = q.DequeueAsync();
                            var d5 = q.DequeueAsync();
                            await q.DequeueAsync();
                            await q.DequeueAsync();
                            await d5;
                            await d4;
                            await d2;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }
                    Console.Write($"({i3}-{q.Count})");
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, TaskScheduler.Current).Unwrap());
            }
            Task.WhenAll(_concurrentTasks).GetAwaiter().GetResult();
            
            Console.WriteLine($"count = {q.Count}, Head = {q?.Tail?.Value}, tail = {q?.Head?.Value}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds*mult*6/(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");
            
            q.Reset();
            foreach (var ioZNode in q)
            {
                Console.Write($"{ioZNode.Value},");
            }
            
            Console.ReadLine();
            throw new Exception("done");
        }
        
        /// <summary>
        /// Tests the semaphore
        /// </summary>
        private static void SemTest() //TODO make unit tests
        {
            CancellationTokenSource asyncTasks = new CancellationTokenSource();

            var capacity = 3;
            var mutex = new IoZeroSemaphoreSlim(asyncTasks, "zero slim", maxBlockers: capacity, initialCount: 0, maxAsyncWork: 0, enableAutoScale: false, enableFairQ: false, enableDeadlockDetection: true);
            //var mutex = new IoZeroRefMut(asyncTasks.Token);

            var releaseCount = 2;
            var enableThrottle = true;
            var waiters = 3;
            var releasers = 3;
            var targetSleep = (long)0;
            var logSpam = 40000;//at least 1

            var targetSleepMult = waiters>1 ? 2 : 1;
            var sw = new Stopwatch();
            var sw2 = new Stopwatch();
            var c = 0;
            long semCount = 0;
            long semPollCount = 0;


            // IoFpsCounter wfps1 = new IoFpsCounter(1000, 10000);
            // IoFpsCounter wfps2 = new IoFpsCounter(1000, 10000);
            // IoFpsCounter ifps1 = new IoFpsCounter(1000, 10000);
            // IoFpsCounter ifps2 = new IoFpsCounter(1000, 10000);
            uint eq1 = 0;
            uint eq2 = 0;
            uint eq3 = 0;
            var eq1_fps = 0.0;
            var eq2_fps = 0.0;
            var eq3_fps = 0.0;
            var dq_fps = new double[10];
            var r = new Random();
            var dq = new uint[10];

            //TaskCreationOptions options = TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously;
            //TaskCreationOptions options = TaskCreationOptions.None;
            TaskCreationOptions options = TaskCreationOptions.DenyChildAttach;

            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
            var t1 = Task.Factory.StartNew(async o =>
             {
                 try
                 {
                     while (waiters>0)
                     {
                         sw.Restart();
                         if (await mutex.WaitAsync().ConfigureAwait(Zc))
                         {
                             var tt = sw.ElapsedMilliseconds;
                             Interlocked.Increment(ref eq1);

                             Action a = (tt - targetSleep * targetSleepMult) switch
                             {
                                 > 5 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                 < -5 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                 _ => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                             };
                             a();

                             //var curTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond - 62135596800000;
                             var curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                             double delta = (curTime - startTime + 1) / 1000.0;

                             if (delta > 5)
                             {
                                 eq1_fps = (eq1 / delta);
                                 eq2_fps = (eq2 / delta);
                                 eq3_fps = (eq3 / delta);
                                 for (int i = 0; i < releasers; i++)
                                 {
                                     dq_fps[i] = (dq[i] / delta);
                                 }
                             }
                             
                             
                             //Console.WriteLine($"T1:{mut.AsyncMutex}({++c}) t = {tt - targetSleep}ms, {fps.Fps(): 00.0}");
                             if (Interlocked.Increment(ref c) % logSpam == 0)
                             {
                                 var totalDq = dq.Aggregate((u, u1) => u + u1);
                                 Console.WriteLine($"T1:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{eq1_fps + eq2_fps + eq3_fps: 0}, ({eq1_fps: 0}, {eq2_fps: 0}, {eq3_fps: 0})], [{totalDq/delta: 0.0} ({dq_fps[0]: 0}, {dq_fps[1]: 0}, {dq_fps[2]: 0}, {dq_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, S = {mutex.ReadyCount}, D = {(int)(eq1 + eq2 + eq3) - (int)totalDq}");
                                 Console.ResetColor();
                             }
                              
                             // if(r.Next()%2 != 0)
                             //     await Task.Delay(1).ConfigureAwait(ZC);
                         }
                         else
                         {
                             //break;
                         }

                     }
                 }
                 catch (Exception e)
                 {
                     Console.WriteLine($"[[1]]:{e}");
                     throw;
                 }
             }, null, options);

            var t2 = Task.Factory.StartNew(async o =>
           {
               try
               {
                   while (waiters>1)
                   {
                        // var block = sem.WaitAsync();
                        // await block.OverBoostAsync().ConfigureAwait(ZC);
                        // if(!block.Result)
                        //     break;
           
                        sw2.Restart();
                       if (await mutex.WaitAsync().ConfigureAwait(Zc))
                       {
                           var tt = sw2.ElapsedMilliseconds;
                           Interlocked.Increment(ref eq2);

                           Action a = (tt - targetSleep * targetSleepMult) switch
                           {
                               > 5 => () => Console.ForegroundColor = ConsoleColor.Red,
                               < -5 => () => Console.ForegroundColor = ConsoleColor.Red,
                               _ => () => Console.ForegroundColor = ConsoleColor.Green,
                           };
                           a();
                           
                           var curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                           double delta = (curTime - startTime + 1) / 1000.0;

                           if (delta > 5)
                           {
                               eq1_fps = (eq1 / delta);
                               eq2_fps = (eq2 / delta);
                               eq3_fps = (eq3 / delta);
                               for (int i = 0; i < releasers; i++)
                               {
                                   dq_fps[i] = (dq[i] / delta);
                               }
                           }
                            
                            curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            delta = (curTime - startTime + 1)/1000.0;
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                            {
                                var totalDq = dq.Aggregate((u, u1) => u + u1);
                                Console.WriteLine($"T2:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{eq1_fps + eq2_fps + eq3_fps: 0}, ({eq1_fps: 0}, {eq2_fps: 0}, {eq3_fps: 0})], [{totalDq/delta: 0.0} ({dq_fps[0]: 0}, {dq_fps[1]: 0}, {dq_fps[2]: 0}, {dq_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, S = {mutex.ReadyCount}, D = {(int)(eq1 + eq2 + eq3) - (int)totalDq}");
                                Console.ResetColor();
                            }
                            
                            // if(r.Next()%2 != 0)
                            //     await Task.Delay(1).ConfigureAwait(ZC);
                       }
                       else
                       {
                           //break;
                       }
           
                   }
               }
               catch (Exception e)
               {
                   Console.WriteLine($"[[2]]:{e}");
                   throw;
               }
           }, null, options);
            
            var t3 = Task.Factory.StartNew(async o =>
           {
               try
               {
                   while (waiters>2)
                   {
                        // var block = sem.WaitAsync();
                        // await block.OverBoostAsync().ConfigureAwait(ZC);
                        // if(!block.Result)
                        //     break;
           
                        sw2.Restart();
                       if (await mutex.WaitAsync().ConfigureAwait(Zc))
                       {
                           var tt = sw2.ElapsedMilliseconds;
                           Interlocked.Increment(ref eq3);
           
                           Action a = (tt - targetSleep * targetSleepMult) switch
                           {
                               > 5 => () => Console.ForegroundColor = ConsoleColor.Red,
                               < -5 => () => Console.ForegroundColor = ConsoleColor.Red,
                               _ => () => Console.ForegroundColor = ConsoleColor.Green,
                           };
                           a();
                            
                            var curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            double delta = (curTime - startTime + 1)/1000.0;
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                            {
                                var totalDq = dq.Aggregate((u, u1) => u + u1);
                                Console.WriteLine($"T3:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{eq1_fps + eq2_fps + eq3_fps: 0}, ({eq1_fps: 0}, {eq2_fps: 0}, {eq3_fps: 0})], [{totalDq/delta: 0.0} ({dq_fps[0]: 0}, {dq_fps[1]: 0}, {dq_fps[2]: 0}, {dq_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, S = {mutex.ReadyCount}, D = {(int)(eq1 + eq2 + eq3) - (int)totalDq}");
                                Console.ResetColor();
                            }
                               
                       }
                       else
                       {
                           break;
                       }
           
                   }
               }
               catch (Exception e)
               {
                   Console.WriteLine($"[[3]]:{e}");
                   throw;
               }
           }, null, options);
            

           for (int i = 0; i < releasers; i++)
           {
               var i1 = i;
               var r3 = Task.Factory.StartNew(async o =>
               {
                   try
                   {
                       var curCount = 1;
                       
                       while (releasers > 0)
                       {
                           if (targetSleep > 0)
                               await Task.Delay((int)targetSleep, asyncTasks.Token).ConfigureAwait(Zc);

                           if (mutex.CurNrOfBlockers >= mutex.Capacity*4/5)
                           {
                               //await Task.Delay(200).ConfigureAwait(ZC);
                           }
                           
                           try
                           {
                               Interlocked.Add(ref semCount, curCount = mutex.Release(releaseCount));

                               Interlocked.Increment(ref semPollCount);
                               Interlocked.Increment(ref dq[i1]);
                           }
                           catch (SemaphoreFullException)
                           {
                               // var f = eq1_fps + eq2_fps + 1;
                               //
                               // var d = mutex.ReadyCount / (f) * 1000.0;
                               // var val = (int)d;
                               // val = Math.Min(1, val);

                               //Console.WriteLine($"Throttling: {val} ms, curCount = {mutex.ReadyCount}");
                               var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                               await Task.Delay(0, asyncTasks.Token).ConfigureAwait(Zc);
                               if (ts.ElapsedMs() > 1200)
                               {
                                   Console.WriteLine($"{ts.ElapsedMs()} ms late");
                               }
                           }
                           catch (TaskCanceledException)
                           {
                               return;
                           }
                           catch (Exception e)
                           {
                               Console.WriteLine(e);
                           }
                       }
                       
                   }
                   catch (Exception e)
                   {
                       Console.WriteLine($"3:{e}");
                       throw;
                   }
               }, asyncTasks.Token, options);
           }

            Console.ReadLine();
            Console.WriteLine("TEARDOWN");
            asyncTasks.Cancel();
            Console.ReadLine();
            Console.WriteLine("Done");
            Console.ReadLine();
        }

        private static void BagTest() //TODO make unit tests
        {
            var bag = new IoBag<IoInt32>("test", 11, true);

            for (int i = 1; i < bag.Capacity; i++)
            {
                bag.Add(i);
            }

            Debug.Assert(bag.Contains((int)bag.Capacity / 2));


            foreach (var i in bag)
            {
                Console.Write($"{i}, ");
                if (i == 7)
                    bag.Add(11);

                if (i == 11)
                    break;
            }

            Console.Write($"\n");
            foreach (var i in bag)
            {
                bag.TryTake(out var r);
                Console.Write($"{r}, ");
            }

            Console.ReadLine();
        }

        private static void BagTest2() //TODO make unit tests
        {
            var bag = new IoHashCodes("test", 11, true);

            for (int i = 1; i < bag.Capacity; i++)
            {
                bag.Add(i);
            }

            Debug.Assert(bag.Contains((int)bag.Capacity / 2));


            foreach (var i in bag)
            {
                Console.Write($"{i}, ");
                if (i == 7)
                    bag.Add(11);

                if (i == 11)
                    break;
            }

            Console.Write($"\n");
            foreach (var i in bag)
            {
                bag.TryTake(out var r);
                Console.Write($"{r}, ");
            }

            Console.ReadLine();
        }

        private static async ValueTask ZeroAsync(int total)
        {
            _running = false;
            await AutoPeeringEventService.ClearAsync().FastPath().ConfigureAwait(Zc);
            Console.WriteLine("#");
            SemaphoreSlim s = new SemaphoreSlim(10);
            int zeroed = 0;
            var sw = Stopwatch.StartNew();

            _nodes?.ToList().ForEach(n =>
            {
                try
                {
                    s.Wait();
                    var task = Task.Run(() =>
                    {
                        n.Zero(new IoNanoprobe("ZeroAsync.Sync"));
                        Interlocked.Increment(ref zeroed);
                    });

                    if (zeroed > 0 && zeroed % 100 == 0)
                    {
                        Console.WriteLine(
                            $"Estimated {TimeSpan.FromMilliseconds((_nodes.Count - zeroed) * (zeroed * 1000 / (sw.ElapsedMilliseconds + 1)))}, zeroed = {zeroed}/{_nodes.Count}");
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine(e);
                }
                finally
                {
                    s.Release();
                }
            });

            Console.WriteLine($"z = {_nodes?.Count(n => n.Zeroed())}/{total}");
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

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("process was exited"));
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("ctrl + c, process was killed"));
                tangleNodeTask.AsTask().Wait();
            }
            else
            {
                var tangleNode = new TangleNode<IoTangleMessage<string>, string>(IoNodeAddress.Create(listenerAddress),
                    (node, ioNetClient, extraData) =>
                        new TanglePeer<string>((TangleNode<IoTangleMessage<string>, string>)node, ioNetClient),
                    TanglePeer<string>.TcpReadAhead);

                var tangleNodeTask = tangleNode.StartAsync();


#pragma warning disable 4014
                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("process was exited"));
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("ctrl + c, process was killed"));
#pragma warning restore 4014
                tangleNodeTask.AsTask().Wait();
            }
        }

        private static Task<CcCollective> CoCoonAsync(CcDesignation ccDesignation, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, List<string> bootStrapAddress)
        {

            var cocoon = new CcCollective(ccDesignation,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                2, 2, 1, 1);

            _nodes.Add(cocoon);

#pragma warning disable 4014

            var t = new Task<CcCollective>(static cocoon =>
            {
                ((CcCollective)cocoon).EmitAsync();
                ((CcCollective)cocoon).StartAsync();
                return (CcCollective)cocoon;
            }, cocoon, TaskCreationOptions.DenyChildAttach);
            return t;
        }
    }
}
