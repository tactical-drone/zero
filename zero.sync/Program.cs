using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MathNet.Numerics.Random;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Filters;
using NLog.Web;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.core.feat.patterns.semaphore;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.core.runtime.scheduler;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace zero.sync
{
    class Program
    {
        private static ConcurrentBag<CcCollective> _nodes = new();
        private static volatile bool _running;
        private static volatile bool _verboseGossip;
        private static volatile bool _startAccounting;
        private static readonly bool Zc = IoNanoprobe.ContinueOnCapturedContext;
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
            Console.WriteLine($"zero ({Environment.OSVersion}: {Environment.MachineName} - dotnet v{Environment.Version}, CPUs = {Environment.ProcessorCount})");

            //Task.Factory.StartNew(async () =>
            //{
            //    await SemTestAsync();
            //    await QueueTestAsync();
            //}, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap().GetAwaiter().GetResult();

            //Tune dotnet for large tests
            ThreadPool.GetMinThreads(out var wt, out var cp);
            ThreadPool.SetMinThreads(wt * 3, cp * 2);

            LogManager.LoadConfiguration("nlog.config");
            var portOffset = 7051;
            
            IHost host = null;
            var grpc = Task.Factory.StartNew(() =>
            {
                host = CreateHostBuilder(args).Build();
                host.Run();
            }, TaskCreationOptions.DenyChildAttach);

            var random = new Random((int)DateTime.Now.Ticks);
            //Tangle("tcp://192.168.1.2:15600");
            var total = 348;
            var maxDrones = 3;
            var maxAdjuncts = 16;
            var boot = true;

            var tasks  = new ConcurrentBag<Task<CcCollective>>();
            if (boot)
                // ReSharper disable once HeuristicUnreachableCode
            {
                var t1 = CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                    $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                    new string[]
                    {
                        $"udp://127.0.0.1:{1235}",
                        $"udp://127.0.0.1:{1236}",
                        $"udp://127.0.0.1:{1237}"
                    }.ToList(), boot);

                var t2 = CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1235}",
                    $"udp://127.0.0.1:{1235}", $"tcp://127.0.0.1:{1235}",
                    $"udp://127.0.0.1:{1235}",
                    new[]
                    {
                        $"udp://127.0.0.1:{1234}",
                        $"udp://127.0.0.1:{1236}",
                        $"udp://127.0.0.1:{1237}"
                    }.ToList(), boot);

                var t3 = CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1236}", $"udp://127.0.0.1:{1236}",
                    $"tcp://127.0.0.1:{1236}", $"udp://127.0.0.1:{1236}",
                    new[]
                    {
                        $"udp://127.0.0.1:{1235}",
                        $"udp://127.0.0.1:{1234}",
                        $"udp://127.0.0.1:{1237}"
                    }.ToList(), boot);

                var t4 = CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1237}",
                    $"udp://127.0.0.1:{1237}", $"tcp://127.0.0.1:{1237}",
                    $"udp://127.0.0.1:{1237}",
                    new[]
                    {
                        $"udp://127.0.0.1:{1234}",
                        $"udp://127.0.0.1:{1235}",
                        $"udp://127.0.0.1:{1236}"
                    }.ToList(), boot);

                tasks = new ConcurrentBag<Task<CcCollective>>
                {
                    t1,t2,t3,t4
                };

#pragma warning disable VSTHRD110 // Observe result of async calls
                Task.Factory.StartNew(() => t1.Start(), TaskCreationOptions.DenyChildAttach).GetAwaiter().GetResult();
                Task.Factory.StartNew(() => t2.Start(), TaskCreationOptions.DenyChildAttach).GetAwaiter().GetResult();
                Task.Factory.StartNew(() => t3.Start(), TaskCreationOptions.DenyChildAttach).GetAwaiter().GetResult();
                Task.Factory.StartNew(() => t4.Start(), TaskCreationOptions.DenyChildAttach).GetAwaiter().GetResult();
#pragma warning restore VSTHRD110 // Observe result of async calls
            }
            else
#pragma warning disable CS0162
            {
                var t1 = CoCoonAsync(CcDesignation.Generate(true), $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                    $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                    new string[]
                    {
                        //$"udp://127.0.0.1:{1235}",
                        //$"udp://127.0.0.1:{1236}",
                        //$"udp://127.0.0.1:{1237}"
                    }.ToList(), false);

                var t2 = CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1235}",
                    $"udp://127.0.0.1:{1235}", $"tcp://127.0.0.1:{1235}",
                    $"udp://127.0.0.1:{1235}",
                    new string[]
                    {
                        $"udp://127.0.0.1:{1234}",
                        //$"udp://127.0.0.1:{1236}",
                        //$"udp://127.0.0.1:{1237}"
                    }.ToList(), false);

                tasks = new ConcurrentBag<Task<CcCollective>>
                {
                    t1,t2
                };
#pragma warning disable VSTHRD110 // Observe result of async calls
                Task.Factory.StartNew(() => t1.Start(), TaskCreationOptions.DenyChildAttach);
                Task.Factory.StartNew(() => t2.Start(), TaskCreationOptions.DenyChildAttach);
#pragma warning restore VSTHRD110 // Observe result of async calls
            }
#pragma warning restore CS0162


            for (var i = 2; i < total; i++)
            {

                tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1234 + portOffset + i}", $"udp://127.0.0.1:{1234 + portOffset + i}", $"tcp://127.0.0.1:{1334 + portOffset + i}", $"udp://127.0.0.1:{1234 + portOffset + i}", new[] { $"udp://127.0.0.1:{1234 + i%4}", $"udp://127.0.0.1:{1234 + (i+1) % 4}" }.ToList()));
                //tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{1234 + portOffset + i}", $"udp://127.0.0.1:{1234 + portOffset + i}", $"tcp://127.0.0.1:{1334 + portOffset + i}", $"udp://127.0.0.1:{1234 + portOffset + i}", new[] { $"udp://127.0.0.1:{1234}" }.ToList()));
                if (tasks.Count % 10 == 0)
                    Console.WriteLine($"Spawned {tasks.Count}/{total}...");
            }

            _ = Task.Factory.StartNew(async () =>
            {
                Console.WriteLine($"Starting auto peering...  {tasks.Count}");
                var c = 1;
                var rateLimit = 9000;
                var injectionCount = 75;
                var rampDelay = 250;
                foreach (var task in tasks)
                {
                    await Task.Factory.StartNew(async () =>
                    {
                        if (task.Status == TaskStatus.Created)
                        {
                            if (c == 1)
                                await Task.Delay(5000);
                            Console.WriteLine($"added {c++}/{tasks.Count}");
                            task.Start();
                        }
                        else
                        {
                            var queen = (CcCollective)task.AsyncState;
                            while (boot && !queen!.Online)
                            {
                                Console.WriteLine("Waiting for queen...");
                                await Task.Delay(1000);
                            }
                            Console.WriteLine($"Queen brought up {queen.Description}");
                        }

                    }, TaskCreationOptions.DenyChildAttach).Unwrap();

                    if (c % injectionCount == 0)
                    {
                        await Task.Delay(rateLimit += 10).ConfigureAwait(false);

                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        if(injectionCount > 40)
                            injectionCount--;
                    }
                    await Task.Delay(rampDelay).ConfigureAwait(false);
                    if(rampDelay -1 > 0)
                        rampDelay -= 1;
                }
            }, TaskCreationOptions.DenyChildAttach);

            _running = true;
            var outBound = 0;
            var inBound = 0;
            var available = 0;
            var logger = LogManager.GetCurrentClassLogger();
            _ = Task.Factory.StartNew(() =>
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
                long perfTime = Environment.TickCount, totalPerfTime = Environment.TickCount;
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
                            //if (ioCcNode.Zeroed())
                            //{
                            //    Thread.Sleep(1000);
                            //    continue;
                            //}

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
                                        Console.WriteLine($"t = {ioCcNode.TotalConnections} ({ioCcNode.IngressCount},{ioCcNode.EgressCount}), [{ioCcNode.Description}] -> {drone.Description} ][ {drone.Adjunct.MetaDesc}, uptime = {drone.UpTime.ElapsedMs():0.0}s");
                                    }
                                }
                            
                                ooutBound += e;
                                oinBound += i;
                                oavailable += ioCcNode.Hub.Neighbors.Values.Count(static n => ((CcAdjunct)n).IsProxy);
                            
                                uptime += ioCcNode.Hub.Neighbors.Values.Select(static n =>
                                {
                                    if (((CcAdjunct)n).IsDroneConnected && ((CcAdjunct)n).AttachTimestamp > 0)
                                        return ((CcAdjunct)n).AttachTimestamp.ElapsedUtc();
                                
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
                            perfTime = Environment.TickCount;
                            try
                            {
                                Console.ForegroundColor = prevPeers <= peers ? ConsoleColor.Green : ConsoleColor.Red;
                                Console.WriteLine($"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / ((double)_nodes.Count * maxDrones * 2) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt}), con = {CcAdjunct.ConnectionTime/(CcAdjunct.ConnectionCount + 1.0):0.0}ms,  [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
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
                            perfTime = Environment.TickCount;
                            opsCheckpoint = ThreadPool.CompletedWorkItemCount;

                            Console.ForegroundColor = ConsoleColor.Green;
                            try
                            {
                                Console.WriteLine($"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / (double)(_nodes.Count * maxDrones * 2) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, w = {-wt + maxwt}, ports = {-cpt + maxcpt}, con = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1.0):0.0}ms, [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
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
                        Console.WriteLine($"Failed... {e.Message}");
                    }

                    
                    Thread.Sleep(30000);
                }
            }, TaskCreationOptions.DenyChildAttach);

            long C = 0;

            string line;
            var ts = Environment.TickCount;
            var tsOrig = Environment.TickCount;
            var cOrig = ThreadPool.CompletedWorkItemCount;
            var c = ThreadPool.CompletedWorkItemCount;
            

            var LS = Environment.TickCount;
            var LSOrig = Environment.TickCount;
            var LOrig = IoZeroScheduler.Zero.CompletedWorkItemCount;
            var LC = IoZeroScheduler.Zero.CompletedWorkItemCount;

            var QS = Environment.TickCount;
            var QSOrig = Environment.TickCount;
            var QOrig = IoZeroScheduler.Zero.CompletedQItemCount;
            var QC = IoZeroScheduler.Zero.CompletedQItemCount;

            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                Console.WriteLine("###");
            };

            Console.CancelKeyPress += (sender, args) =>
            {
                args.Cancel = true;
                ZeroAsync(total).AsTask().GetAwaiter();
            };

            while ((line = Console.ReadLine()) != null && !line.StartsWith("quit"))
            {
                try
                {
                    if (line == "gc")
                    {
                        var pinned = GC.GetGCMemoryInfo(GCKind.Any).PinnedObjectsCount;
                        GC.Collect(GC.MaxGeneration);
                        Console.WriteLine($"Pinned was = {pinned}, now {GC.GetGCMemoryInfo(GCKind.Any).PinnedObjectsCount}");
                    }
                
                    if (line == "quit")
                        break;

                    if (line == "t")
                    {
                        Console.WriteLine($"load = {IoZeroScheduler.Zero.Load}({IoZeroScheduler.Zero.LoadFactor * 100:0.0}%), q time = {ThreadPool.PendingWorkItemCount / ((ThreadPool.CompletedWorkItemCount - c) / (double)ts.ElapsedMs()):0}ms, threads = {ThreadPool.ThreadCount}({IoZeroScheduler.Zero.ThreadCount}), p = {ThreadPool.PendingWorkItemCount}({IoZeroScheduler.Zero.WLength}), t = {ThreadPool.CompletedWorkItemCount}, {(ThreadPool.CompletedWorkItemCount - cOrig) / (double)tsOrig.ElapsedMsToSec():0.0} ops, c = {ThreadPool.CompletedWorkItemCount-c}, {(ThreadPool.CompletedWorkItemCount-c)/(double)ts.ElapsedMsToSec():0.0} tps");
                        ts = Environment.TickCount;
                        c = ThreadPool.CompletedWorkItemCount;
                    }

                    if (line == "L")
                    {
                        var active = IoZeroScheduler.Zero.Active;
                        var blocked = IoZeroScheduler.Zero.Blocked;
                        var free = IoZeroScheduler.Zero.Free;
                        var qactive = IoZeroScheduler.Zero.QActive;
                        try
                        {
                            Console.WriteLine($"[{free.Count}/{blocked.Count}/{active.Count}] load = {IoZeroScheduler.Zero.Load}({IoZeroScheduler.Zero.LoadFactor * 100:0.0}%), q time = {IoZeroScheduler.Zero.WLength / ((IoZeroScheduler.Zero.CompletedWorkItemCount - LC) / (double)LS.ElapsedMs()):0}ms, workers = {IoZeroScheduler.Zero.ThreadCount}, p = {IoZeroScheduler.Zero.WLength}, C = {IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig}, {(IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig) / (double)LSOrig.ElapsedMsToSec():0.0} ops, c = {IoZeroScheduler.Zero.CompletedWorkItemCount - LC}, {(IoZeroScheduler.Zero.CompletedWorkItemCount - LC) / (double)LS.ElapsedMsToSec():0.0} tps");
                            Console.WriteLine($"[{IoZeroScheduler.Zero.QFree.Count}/{IoZeroScheduler.Zero.QBlocked.Count}/{qactive.Count}] load = {IoZeroScheduler.Zero.QLoad}({IoZeroScheduler.Zero.QLoadFactor * 100:0.0}%), q time = {IoZeroScheduler.Zero.QLength / ((IoZeroScheduler.Zero.QLength - QC) / (double)QS.ElapsedMs()):0}ms, poll% = {((IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig - LC) - (IoZeroScheduler.Zero.CompletedQItemCount - QC)) / (double)(IoZeroScheduler.Zero.CompletedWorkItemCount - LC) * 100.0:0.0}%, ({(IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig - (IoZeroScheduler.Zero.CompletedQItemCount - QOrig)) / (double)(IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig) * 100.0:0.00}%) ,queens = {IoZeroScheduler.Zero.QThreadCount}, p = {IoZeroScheduler.Zero.QLength}, C = {IoZeroScheduler.Zero.CompletedQItemCount - QOrig}, {(IoZeroScheduler.Zero.CompletedWorkItemCount - QOrig) / (double)QSOrig.ElapsedMsToSec():0.0} ops, c = {IoZeroScheduler.Zero.CompletedQItemCount- QC}, {(IoZeroScheduler.Zero.CompletedQItemCount - QC) / (double)QS.ElapsedMsToSec():0.0} tps");
                            QS = Environment.TickCount;
                            QC = IoZeroScheduler.Zero.CompletedQItemCount;
                            LS = Environment.TickCount;
                            LC = IoZeroScheduler.Zero.CompletedWorkItemCount;
                        }
                        finally
                        {
                            IoZeroScheduler.Zero.Return(active);
                            IoZeroScheduler.Zero.Return(free);
                            IoZeroScheduler.Zero.Return(blocked);
                            IoZeroScheduler.Zero.Return(qactive);
                        }
                    }

                    if (line.StartsWith("logf"))
                    {
                        try
                        {
                            var line1 = line;
                            var res = line1.Split(' ')[1];
                            if (!string.IsNullOrEmpty(res))
                            {
                                LogManager.Configuration.LoggingRules.Last().Filters.Add(new WhenMethodFilter(logEvent =>
                                {
                                    var res = logEvent.Message.Contains(line1.Split(' ')[1]);

                                    return res
                                        ? FilterResult.Log
                                        : FilterResult.Ignore;
                                }));
                                LogManager.Configuration = LogManager.Configuration;
                            }
                        }
                        catch { }
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

                    if (line.StartsWith("loadTest"))
                    {
                        var n = _nodes.Where(n => !n.ZeroDrone).ToArray();
                        n[Random.Shared.Next(0,n.Length - 1)].BootAsync(0).AsTask().GetAwaiter().GetResult();
                        n[Random.Shared.Next(0, n.Length - 1)].BootAsync(0).AsTask().GetAwaiter().GetResult();
                        n[Random.Shared.Next(0, n.Length - 1)].BootAsync(0).AsTask().GetAwaiter().GetResult();
                        n[Random.Shared.Next(0, n.Length - 1)].BootAsync(0).AsTask().GetAwaiter().GetResult();
                        Console.WriteLine("ZERO CORE best case horizontal scale cluster TCP/IP (DDoS) pressure test started... make sure your CPU has enough juice, this test will redline your kernel and hang your OS");
                    }

                    if (line.StartsWith("gr"))
                    {
                        foreach (var ccCollective in _nodes)
                        {
                            ccCollective.Drones.ForEach(d=>d.ToggleAccountingBit());
                        }
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
                                tasks.ToList().ForEach(t=>t.Result.ClearDupBuf());
                                gossipTasks.Clear();
                                AutoPeeringEventService.QueuedEvents[0].ZeroManagedAsync<object>().AsTask().GetAwaiter().GetResult();
                            }

                            long v = 1;
                            _rampDelay = 5000;
                            _rampTarget = 1000;
                            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            var threads = 3;
                            _running = true;
                            for (int i = 0; i < threads; i++)
                            {
                                var i1 = i;
                                gossipTasks.Add(Task.Factory.StartNew(async () =>
                                {
                                    while (!_startAccounting)
                                        await Task.Delay(1000);
                                    Console.WriteLine($"Starting accounting... {tasks.Count}");
                                    await Task.Delay(random.Next(1000 / threads) * i1);
                                    while (_running)
                                    {
                                        if (!_startAccounting)
                                        {
                                            await Task.Delay(1000);
                                            continue;
                                        }

                                        foreach (var t in tasks)
                                        {
                                            if (!_startAccounting)
                                                break;
                                            var ts2 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                            if (!await ((CcCollective)t.AsyncState)!.BootAsync(Interlocked.Increment(ref v)).FastPath()
                                                    )
                                            {
                                                if (_verboseGossip)
                                                    Console.WriteLine($"* - {ts2.ElapsedMs()}ms");
                                                continue;
                                            }

                                            if (_verboseGossip)
                                                Console.WriteLine($". - {ts2.ElapsedMs()}ms");

                                            //ramp
                                            if (_rampDelay > _rampTarget)
                                            {
                                                Interlocked.Decrement(ref _rampDelay);
                                                //Console.WriteLine($"ramp = {_rampDelay}");
                                            }
                                        
                                            await Task.Delay(_rampDelay).ConfigureAwait(false);

                                            if (Interlocked.Increment(ref C) % 5000 == 0)
                                            {
                                                Console.WriteLine($"{C / ((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start) / 1000.0):0.0} g/ps");
                                                Interlocked.Exchange(ref C, 0);
                                                start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                            }
                                        }
                                    }
                                    Console.WriteLine("Stopped gossip...");
                                }, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.DenyChildAttach));
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
                            var nodes= _nodes.Where(n=>!n.ZeroDrone).ToList();
                            try
                            {
                            
                                var min = nodes.Min(n => n.EventCount);
                                var max = nodes.Max(n => n.EventCount);
                                //var nmax = max;

                                var ave = nodes.Average(n => n.EventCount);
                                var err = nodes.Select(n => Math.Abs(n.EventCount - ave)).Average();
                                var r = 0;
                                var target = 0;
                                while (err > (target = 50 - r/100 * 40) && r++ < 100 && nodes.Count > 0)
                                {
                                    nodes = nodes.Where(n => Math.Abs(n.EventCount - ave) < target).ToList();
                                    ave = nodes.Average(n => n.EventCount);
                                    err = nodes.Select(n => Math.Abs(n.EventCount - ave)).Average();
                                    //nmax = nodes.Max(n => n.EventCount);
                                }
                            
                                Console.WriteLine($"zero liveness at {nodes.Count()/((double)_nodes.Count - 4)*100:0.00}%, {nodes.Count()}/{_nodes.Count-4}, min/max = [{min},{max}], ave = {ave:0.0}, err = {err:0.0}, r = {r}");
                            }
                            catch (Exception) { }
                        }
                    }

                    if (line.StartsWith("stream"))
                    {
                        AutoPeeringEventService.ToggleActiveAsync().GetAwaiter().GetResult();
                        Console.WriteLine($"event stream = {(AutoPeeringEventService.Operational? "On":"Off")}");
                    }

                    if (line.StartsWith("zero"))
                    {
                        ZeroAsync(total).AsTask().ContinueWith(_ =>
                        {
                            if (_nodes != null)
                            {
                                Console.WriteLine($"z = {_nodes.Count(n => n.Zeroed())}/{total}");
                                _nodes.Clear();
                                _nodes = null;
                                tasks?.Clear();
                                tasks = null;
                            }
                        }).GetAwaiter().GetResult();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            };
            

            _running = false;
            tasks?.Clear();
            tasks = null;
            _nodes?.Clear();
            _nodes = null;

            var s = host.StopAsync();
            host = null;
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
        private static async Task QueueTestAsync() //TODO make unit tests
        {
            await Task.Delay(1000);
            IoQueue<int> q = new("test", 300, 400);
            var head = q.PushBackAsync(2).FastPath().GetAwaiter().GetResult();
            q.PushBackAsync(1).FastPath().GetAwaiter();
            q.EnqueueAsync(3).FastPath().GetAwaiter();
            q.EnqueueAsync(4).FastPath().GetAwaiter();
            var five = q.PushBackAsync(5).FastPath().GetAwaiter().GetResult();
            q.EnqueueAsync(6).FastPath().GetAwaiter();
            q.EnqueueAsync(7).FastPath().GetAwaiter();
            q.EnqueueAsync(8).FastPath().GetAwaiter();
            var tail = q.EnqueueAsync(9).FastPath().GetAwaiter().GetResult();

            Console.WriteLine("Init");
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }
            Console.WriteLine("\nDQ mid");
            q.RemoveAsync(five).FastPath().GetAwaiter();
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
            q.RemoveAsync(q.Head).FastPath().GetAwaiter();
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
            q.RemoveAsync(q.Tail).FastPath().GetAwaiter();
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
            q.DequeueAsync().FastPath().GetAwaiter();
            q.RemoveAsync(q.Head).FastPath().GetAwaiter();
            q.RemoveAsync(q.Head).FastPath().GetAwaiter();
            q.DequeueAsync().FastPath().GetAwaiter();

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
                q.RemoveAsync(q.Head).FastPath().GetAwaiter();
                q.RemoveAsync(q.Head).FastPath().GetAwaiter();    
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
            var rounds = 45;
            var mult = 1000000;
            //var rounds = 5;
            //var mult = 1000;
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
                            Console.WriteLine($"Failed... {e.Message}");
                        }
                    }
                    Console.Write($"({i3}-{q.Count})");
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap());
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
        private static async Task SemTestAsync() //TODO make unit tests
        {
            await Task.Delay(1000);
            var asyncTasks = new CancellationTokenSource();

            var capacity = 3;

            //.NET RUNTIME REFERENCE MUTEX FOR TESTING
            //var mutex = new IoZeroRefMut(asyncTasks.Token);
            var mutex = new IoZeroSemaphoreSlim(asyncTasks, "zero slim", maxBlockers: capacity, initialCount: 0, maxAsyncWork: 0, enableAutoScale: false, enableFairQ: false, enableDeadlockDetection: true);

            var releaseCount = 2;
            var waiters = 3;
            var releasers = 2;
            var targetSleep = (long)0;
            var logSpam = 40000;//at least 1

            var targetSleepMult = waiters > 1 ? 2 : 1;
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
            var dq = new long[10];

            //TaskCreationOptions options = TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously;
            //TaskCreationOptions options = TaskCreationOptions.None;
            TaskCreationOptions options = TaskCreationOptions.DenyChildAttach;

            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
            var t1 = Task.Factory.StartNew(async () =>
             {
                 try
                 {
                     while (waiters>0)
                     {
                         sw.Restart();
                         if (await mutex.WaitAsync().FastPath())
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
             }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Current);

            var t2 = Task.Factory.StartNew(async () =>
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
                       if (await mutex.WaitAsync().FastPath())
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
           }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Current);
            
            var t3 = Task.Factory.StartNew(async () =>
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
                       if (await mutex.WaitAsync().FastPath())
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
           }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Current);
            

           for (int i = 0; i < releasers; i++)
           {
               var i1 = i;
               var r3 = Task.Factory.StartNew(async o =>
               {
                   try
                   {
                       var curCount = 0;
                       
                       while (releasers > 0)
                       {
                           try
                           {
                               if ((curCount = mutex.Release(releaseCount)) > 0)
                               {
                                   Interlocked.Add(ref semCount, curCount);
                                   Interlocked.Increment(ref semPollCount);
                                   Interlocked.Add(ref dq[i1], releaseCount);
                               }
                               else
                               {
                                   var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                   await Task.Delay(1, asyncTasks.Token);
                               }

                           }
                           catch (SemaphoreFullException)
                           {


                           }
                           catch (TaskCanceledException)
                           {
                               return;
                           }
                           catch (Exception e)
                           {
                               Console.WriteLine($"Failed... {e.Message}");
                           }
                           finally
                           {
                               if (targetSleep > 0)
                                   await Task.Delay((int)targetSleep, asyncTasks.Token);
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

        private static async ValueTask ZeroAsync(int total)
        {
            _running = false;
            await AutoPeeringEventService.ClearAsync();
            Console.WriteLine("#");
            SemaphoreSlim s = new (10);
            int zeroed = 0;
            var sw = Stopwatch.StartNew();

            _nodes?.ToList().ForEach(n =>
            {
                try
                {
                    s.Wait();
                    var task = Task.Run(async () =>
                    {
                        await n.Zero(null,"MAIN TEARDOWN");
                        Interlocked.Increment(ref zeroed);
                    });

                    if (zeroed > 0 && zeroed % 100 == 0)
                    {
                        Console.WriteLine($"Estimated {TimeSpan.FromMilliseconds((_nodes.Count - zeroed) * (zeroed * 1000 / (sw.ElapsedMilliseconds + 1)))}, zeroed = {zeroed}/{_nodes.Count}");
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine($"Failed... {e.Message}");
                }
                finally
                {
                    s.Release();
                }
            });

            GC.Collect(GC.MaxGeneration);
            Console.WriteLine($"z = {_nodes?.Count(n => n.Zeroed())}/{total}");
        }

//        private static void Tangle(string listenerAddress)
//        {

//            if (Entangled<string>.Optimized)
//            {
//                var tangleNode = new TangleNode<IoTangleMessage<byte[]>, byte[]>(IoNodeAddress.Create(listenerAddress),
//                    (node, ioNetClient, extraData) =>
//                        new TanglePeer<byte[]>((TangleNode<IoTangleMessage<byte[]>, byte[]>)node, ioNetClient),
//                    TanglePeer<byte[]>.TcpReadAhead);

//#pragma warning disable 4014
//                var tangleNodeTask = tangleNode.StartAsync();

//                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("process was exited"));
//                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("ctrl + c, process was killed"));
//                tangleNodeTask.AsTask().Wait();
//            }
//            else
//            {
//                var tangleNode = new TangleNode<IoTangleMessage<string>, string>(IoNodeAddress.Create(listenerAddress),
//                    (node, ioNetClient, extraData) =>
//                        new TanglePeer<string>((TangleNode<IoTangleMessage<string>, string>)node, ioNetClient),
//                    TanglePeer<string>.TcpReadAhead);

//                var tangleNodeTask = tangleNode.StartAsync();


//#pragma warning disable 4014
//                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("process was exited"));
//                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Zero(new IoNanoprobe("ctrl + c, process was killed"));
//#pragma warning restore 4014
//                tangleNodeTask.AsTask().Wait();
//            }
//        }

        private static Task<CcCollective> CoCoonAsync(CcDesignation ccDesignation, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, IEnumerable<string> bootStrapAddress, bool zeroDrone = false)
        {

            var cocoon = new CcCollective(ccDesignation,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                2, 2, 1, 1, zeroDrone);

            _nodes.Add(cocoon);

            var t = new Task<CcCollective>(static cocoon =>
            {
                ((CcCollective)cocoon).Emit();
                ((CcCollective)cocoon).StartAsync().AsTask().GetAwaiter().GetResult();
                return (CcCollective)cocoon;
            }, cocoon, TaskCreationOptions.DenyChildAttach);
            return t;
        }
    }
}
