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
using Microsoft.VisualStudio.Threading;
using NLog;
using NLog.Filters;
using NLog.Web;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.events.services;
using zero.cocoon.identity;
using zero.core.feat.patterns.time;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;
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
        private static int _rampDelay = 300;
        private static int _rampTarget = 40;
        public static IHostBuilder CreateHostBuilder(int port) =>
            Host.CreateDefaultBuilder(null)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder
                        .ConfigureKestrel(options =>
                        {
                            options.ListenAnyIP(port,
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

            var total = 0;
            int localPort = - 1;
            int [] remotePort = new int[10];

            if (args.Length > 0 && !string.IsNullOrEmpty(args[0]))
                int.TryParse(args[0], out localPort);

            if (args.Length > 1 && !string.IsNullOrEmpty(args[1]))
            {
                var l = 1;
                while (l < args.Length)
                {
                    int.TryParse(args[l], out remotePort[l - 1]);
                    l++;
                }
            }
            LogManager.LoadConfiguration("nlog.config");

            if (localPort != -1)
            {
                LogManager.Configuration.Variables["zeroLogLevel"] = "trace";
                LogManager.ReconfigExistingLoggers();
            }


            static void Bootstrap(out ConcurrentBag<CcCollective> concurrentBag, int total, int portOffset = 7051, int localPort = -1, int[] remotePort = null)
            {
                var maxDrones = 3;
                var maxAdjuncts = 9;
                
                var oldBoot = localPort == -1;
                var queens = false;

                concurrentBag = new ConcurrentBag<CcCollective>();
                if (oldBoot)
                {
                    if (queens)
                        // ReSharper disable once HeuristicUnreachableCode
                    {
                        CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1234}",
                            $"udp://127.0.0.1:{1234}",
                            $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                            new string[]
                            {
                                $"udp://127.0.0.1:{1235}",
                                $"udp://127.0.0.1:{1236}",
                                $"udp://127.0.0.1:{1237}"
                            }.ToList(), queens);

                        CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1235}",
                            $"udp://127.0.0.1:{1235}", $"tcp://127.0.0.1:{1235}",
                            $"udp://127.0.0.1:{1235}",
                            new[]
                            {
                                $"udp://127.0.0.1:{1234}",
                                $"udp://127.0.0.1:{1236}",
                                $"udp://127.0.0.1:{1237}"
                            }.ToList(), queens);

                        CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1236}",
                            $"udp://127.0.0.1:{1236}",
                            $"tcp://127.0.0.1:{1236}", $"udp://127.0.0.1:{1236}",
                            new[]
                            {
                                $"udp://127.0.0.1:{1235}",
                                $"udp://127.0.0.1:{1234}",
                                $"udp://127.0.0.1:{1237}"
                            }.ToList(), queens);

                        CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1237}",
                            $"udp://127.0.0.1:{1237}", $"tcp://127.0.0.1:{1237}",
                            $"udp://127.0.0.1:{1237}",
                            new[]
                            {
                                $"udp://127.0.0.1:{1234}",
                                $"udp://127.0.0.1:{1235}",
                                $"udp://127.0.0.1:{1236}"
                            }.ToList(), queens);
                    }
                    else
#pragma warning disable CS0162
                    {
                        StartCocoon(CoCoon(CcDesignation.Generate(true), $"tcp://127.0.0.1:{1234}",
                            $"udp://127.0.0.1:{1234}",
                            $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                            new string[]
                            {
                                $"udp://127.0.0.1:{1235}",
                                //$"udp://127.0.0.1:{1236}",
                                //$"udp://127.0.0.1:{1237}"
                            }.ToList(), false));

                        StartCocoon(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1235}",
                            $"udp://127.0.0.1:{1235}", $"tcp://127.0.0.1:{1235}",
                            $"udp://127.0.0.1:{1235}",
                            new string[]
                            {
                                $"udp://127.0.0.1:{1234}",
                                //$"udp://127.0.0.1:{1236}",
                                //$"udp://127.0.0.1:{1237}"
                            }.ToList(), false));
                    }
                }
                else
                {
                    var count = remotePort.Count(p => p > 0);
                    StartCocoon(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{localPort}",
                        $"udp://127.0.0.1:{localPort}",
                        $"tcp://127.0.0.1:{localPort}", $"udp://127.0.0.1:{localPort}",
                        remotePort.Take(count).Select(p => $"udp://127.0.0.1:{p}").ToList(), false));

                    Console.WriteLine($"starting udp://127.0.0.1:{localPort} -> {string.Join(", ", remotePort.Take(count).Select(port => $"udp://127.0.0.1:{port}"))}");

                }
#pragma warning restore CS0162

                portOffset += 2;
                for (var i = 0; i < total; i++)
                {
                    var range = 20;
                    var o1 = Random.Shared.Next(portOffset + Math.Max(0, i - range), portOffset + i);
                    var o2 = Random.Shared.Next(portOffset + Math.Max(0, i - range), portOffset + i);
                    var o3 = Random.Shared.Next(portOffset + Math.Max(0, i - range), portOffset + i);
                    var o4 = Random.Shared.Next(portOffset + Math.Max(0, i - range), portOffset + i);
                    string extra = i == 0 ? $"udp://127.0.0.1:{1235}" : $"udp://127.0.0.1:{1234 + portOffset + i - 1}";

                    concurrentBag.Add(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1234 + portOffset + i}",
                        $"udp://127.0.0.1:{1234 + portOffset + i}", $"tcp://127.0.0.1:{1334 + portOffset + i}",
                        $"udp://127.0.0.1:{1234 + portOffset + i}",
                        new[]
                        {
                            //$"udp://127.0.0.1:{1234 + portOffset + i - 1}",
                            extra, 
                            $"udp://127.0.0.1:{1234 + o1}", $"udp://127.0.0.1:{1234 + o2}",
                            $"udp://127.0.0.1:{1234 + o3}", $"udp://127.0.0.1:{1234 + o4}"
                        }.ToList()));

                    if (concurrentBag.Count % 10 == 0)
                        Console.WriteLine($"Spawned {concurrentBag.Count}/{total}");
                }

                var bag = concurrentBag;
                _ = Task.Factory.StartNew(async () =>
                {
                    Console.WriteLine($"Starting auto peering...  {bag.Count}");
                    var c = 0;
                    var rateLimit = 5000;
                    var injectionCount = Math.Max(1,bag.Count / 20);
                    var rampDelay = 50;
                    foreach (var cocoon in bag.OrderBy(e => e.Serial))
                    {
                        await Task.Delay(rampDelay).ConfigureAwait(false);
                        if (rampDelay> 50)
                            rampDelay -= 1;

                        Console.WriteLine($"addeding {c}/{bag.Count}");
                        StartCocoon(cocoon);
                        Console.WriteLine($"added {++c}/{bag.Count}");

                        if (c % injectionCount == 0)
                        {
                            Console.WriteLine($"Provisioned {c}/{total}");
                            Console.WriteLine($"Provisioned {c}/{total}");
                            Console.WriteLine($"Provisioned {c}/{total}");
                            Console.WriteLine($"Provisioned {c}/{total}");
                            Console.WriteLine($"Provisioned {c}/{total}");
                            Console.WriteLine($"Provisioned {c}/{total}");

                            await Task.Delay(rateLimit -= 0).ConfigureAwait(false);

                            //if (injectionCount > 50)
                            //    injectionCount--;
                        }

                    }
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

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
                                            var drone = (CcDrone)d;
                                            Console.WriteLine(
                                                $"t = {ioCcNode.TotalConnections} ({ioCcNode.IngressCount},{ioCcNode.EgressCount}), [{ioCcNode.Description}] -> {drone.Description} ][ {drone.Adjunct.MetaDesc}, uptime = {drone.UpTime.ElapsedMs():0.0}s");
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
                                    Console.WriteLine(
                                        $"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / ((double)_nodes.Count * maxDrones * 2) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt}), con = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1.0):0.0}ms,  [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
                                }
                                catch
                                {
                                }
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
                                    Console.WriteLine(
                                        $"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / (double)(_nodes.Count * maxDrones * 2) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, w = {-wt + maxwt}, ports = {-cpt + maxcpt}, connect time = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1.0):0.0}ms, [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
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
                            Console.WriteLine($"Failed... {e.Message} {e.InnerException}");
                        }


                        Thread.Sleep(30000);
                    }
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
            }

            Console.WriteLine($"zero ({Environment.OSVersion}: {Environment.MachineName} - dotnet v{Environment.Version}, CPUs = {Environment.ProcessorCount})");

            IoTimer.Make(make: static (delta,signal, token) =>
            {
#pragma warning disable VSTHRD101
                var t = new Thread(static async state =>
                {
                    var (delta,signal,token) = (ValueTuple<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken>) state;

                    signal.RunContinuationsAsynchronouslyAlways = true;
                    var p = new PeriodicTimer(delta);
                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            if (await p.WaitForNextTickAsync(token).FastPath())
                                signal.SetResult(Environment.TickCount);
                            else
                            {
                                signal.SetException(new OperationCanceledException());
                            }
                                
                        }
                        catch 
                        {
                            try
                            {
                                signal.Reset();
                                signal.SetException(new OperationCanceledException());
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                        }
                    }
                });
#pragma warning restore VSTHRD101
                t.Start((delta, signal, token));
            });

            var j = new JoinableTaskFactory(new JoinableTaskContext());
            {
                var t = j.RunAsync(async () =>
                {
                    await SemTestAsync();
                    //await QueueTestAsync();
                    //await ZeroQTestAsync();
                    //await BagTestAsync();)
                });
                t.Join();
            }

            //var testDone = false;
            //var testTask = Task.Factory.StartNew(async () =>
            //{
            //    Console.Write("1->");
            //    await SemTestAsync();
            //    //await QueueTestAsync();
            //    //await ZeroQTestAsync();
            //    //await BagTestAsync();
            //}, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap();



            //var spinWait = new SpinWait();

            //while (!testDone)
            //{
            //    spinWait.SpinOnce();
            //    if (spinWait.Count % 2000 == 0)
            //    {
            //        Thread.Sleep(2000);
            //    }
            //}



            //Tune dotnet for large tests
            ThreadPool.GetMinThreads(out var wt, out var cp);
            ThreadPool.SetMinThreads(wt * 3, cp * 2);

            Console.WriteLine($"local = {localPort}, remote = {remotePort}");

            IHost host = null;
            var port = localPort;
            var grpc = Task.Factory.StartNew(() =>
            {
                host = CreateHostBuilder(AutoPeeringEventService.Port = port == -1 ? 27021 : port + 1).Build();
                host.Run();
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

            Bootstrap(out var tasks, total, 0, localPort, remotePort);

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

            var AS = Environment.TickCount;
            var AC = IoZeroScheduler.Zero.CompletedAsyncCount;

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
                    if (line.Contains("boot"))
                    {
                        var tokens = line.Split(' ');
                        if (tokens.Length > 1)
                            total = int.Parse(tokens[1]);

                        Bootstrap(out tasks, total);
                    }
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
                        Console.WriteLine($"load = {IoZeroScheduler.Zero.Load}({IoZeroScheduler.Zero.LoadFactor * 100:0.0}%), q time = {ThreadPool.PendingWorkItemCount / ((ThreadPool.CompletedWorkItemCount - c) / (double)ts.ElapsedMs()):0}ms, threads = {ThreadPool.ThreadCount}({IoZeroScheduler.Zero.ThreadCount}), p = {ThreadPool.PendingWorkItemCount}({IoZeroScheduler.Zero.QLength}), t = {ThreadPool.CompletedWorkItemCount}, {(ThreadPool.CompletedWorkItemCount - cOrig) / (double)tsOrig.ElapsedMsToSec():0.0} ops, c = {ThreadPool.CompletedWorkItemCount-c}, {(ThreadPool.CompletedWorkItemCount-c)/(double)ts.ElapsedMsToSec():0.0} tps");
                        ts = Environment.TickCount;
                        c = ThreadPool.CompletedWorkItemCount;
                    }

                    if (line == "L")
                    {
                        Console.WriteLine($"ZERO SCHEDULER: load = {IoZeroScheduler.Zero.Load}({IoZeroScheduler.Zero.LoadFactor * 100:0.0}%), q time = {IoZeroScheduler.Zero.QLength / ((IoZeroScheduler.Zero.CompletedWorkItemCount - LC) / (double)LS.ElapsedMs()):0}ms, workers = {IoZeroScheduler.Zero.ThreadCount}, p = {IoZeroScheduler.Zero.QLength}, C = {IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig}, {(IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig) / (double)LSOrig.ElapsedMsToSec():0.0} ops, c = {IoZeroScheduler.Zero.CompletedWorkItemCount - LC}, {(IoZeroScheduler.Zero.CompletedWorkItemCount - LC) / (double)LS.ElapsedMsToSec():0.0} tps");
                        Console.WriteLine($"[{IoZeroScheduler.Zero.Load}/{IoZeroScheduler.Zero.AsyncLoad}/{IoZeroScheduler.Zero.ForkLoad}/{IoZeroScheduler.Zero.Capacity}] load = {IoZeroScheduler.Zero.Load/ IoZeroScheduler.Zero.Capacity * 100:0.0}%, w time = {IoZeroScheduler.Zero.QTime:0.0}ms, aq time = {IoZeroScheduler.Zero.AQTime:0.0}ms, poll% = {((IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig - LC) - (IoZeroScheduler.Zero.CompletedQItemCount - QC)) / (double)(IoZeroScheduler.Zero.CompletedWorkItemCount - LC) * 100.0:0.0}%, ({(IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig - (IoZeroScheduler.Zero.CompletedQItemCount - QOrig)) / (double)(IoZeroScheduler.Zero.CompletedWorkItemCount - LOrig) * 100.0:0.00}%), p = {IoZeroScheduler.Zero.QLength},  r = {IoZeroScheduler.Zero.RLength}, F = {IoZeroScheduler.Zero.ForkLoad}, C = {IoZeroScheduler.Zero.CompletedQItemCount - QOrig}, {(IoZeroScheduler.Zero.CompletedWorkItemCount - QOrig) / (double)QSOrig.ElapsedMsToSec():0.0} ops, c = {IoZeroScheduler.Zero.CompletedQItemCount- QC}, {(IoZeroScheduler.Zero.CompletedQItemCount - QC) / (double)QS.ElapsedMsToSec():0.0} tps, {(IoZeroScheduler.Zero.CompletedAsyncCount - AC) / (double)AS.ElapsedMsToSec():0.0} async/s ({IoZeroScheduler.Zero.CompletedAsyncCount - AC})[{IoZeroScheduler.Zero.CompletedAsyncCount}]");
                        QS = Environment.TickCount;
                        AS = Environment.TickCount;
                        LS = Environment.TickCount;
                        QC = IoZeroScheduler.Zero.CompletedQItemCount;
                        LC = IoZeroScheduler.Zero.CompletedWorkItemCount;
                        AC = IoZeroScheduler.Zero.CompletedAsyncCount;
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
                        IoZeroScheduler.Zero.LoadAsyncCallback(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(1).FastPath());
                        //IoZeroScheduler.Zero.LoadAsyncCallback(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(0).FastPath());
                        //IoZeroScheduler.Zero.LoadAsyncCallback(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(0).FastPath());
                        //IoZeroScheduler.Zero.LoadAsyncCallback(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(0).FastPath());
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
                                IoZeroScheduler.Zero.LoadAsyncCallback(async () =>
                                {
                                    await Task.WhenAll(gossipTasks.ToArray());
                                    gossipTasks.Clear();
                                    await AutoPeeringEventService.ClearAsync().FastPath();
                                });
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
                                var bag = tasks;
                                gossipTasks.Add(Task.Factory.StartNew(async () =>
                                {
                                    while (!_startAccounting)
                                        await Task.Delay(1000);
                                    Console.WriteLine($"Starting accounting... {bag.Count}");
                                    await Task.Delay(50 + i1);
                                    while (_running)
                                    {
                                        if (!_startAccounting)
                                        {
                                            await Task.Delay(1000);
                                            continue;
                                        }

                                        foreach (var t in bag)
                                        {
                                            if (!_startAccounting)
                                                break;
                                            var ts2 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                            if (!await t!.BootAsync(Interlocked.Increment(ref v)).FastPath())
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
                                    Console.WriteLine("Stopped gossip");
                                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default));
                                _startAccounting = true;
                            }
                            Console.WriteLine($"Stopped auto peering  {tasks.Count}");
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
                        IoZeroScheduler.Zero.LoadAsyncCallback(async () => await AutoPeeringEventService.ToggleActiveAsync());
                        
                        Console.WriteLine($"event stream = {(AutoPeeringEventService.Operational? "On":"Off")}");
                    }

                    if (line.StartsWith("zero"))
                    {
                        IoZeroScheduler.Zero.LoadAsyncCallback(async () =>
                        {
                            await ZeroAsync(total).FastPath();
                            
                            if (_nodes != null)
                            {
                                _nodes.Clear();
                                tasks.Clear();
                                await AutoPeeringEventService.ClearAsync().FastPath();
                                Console.WriteLine($"z = {_nodes.Count(n => n.Zeroed())}/{total}");
                            }
                        });
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
            IoZeroScheduler.Zero.Dispose();
            
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
            //var concurrencyLevel = Environment.ProcessorCount * 2;
            var concurrencyLevel = 2;
            IoQueue<int> q = new("test", 4096, concurrencyLevel);
            if (!q.Configuration.HasFlag(IoQueue<int>.Mode.BackPressure))
            {


                var head = await q.PushBackAsync(2).FastPath();
                await q.PushBackAsync(1).FastPath();
                await q.EnqueueAsync(3).FastPath();
                await q.EnqueueAsync(4).FastPath();
                var five = await q.PushBackAsync(5).FastPath();
                await q.EnqueueAsync(6).FastPath();
                await q.EnqueueAsync(7).FastPath();
                await q.EnqueueAsync(8).FastPath();
                var tail = await q.EnqueueAsync(9).FastPath();

                Console.WriteLine("Init");
                foreach (var ioZNode in q)
                {
                    Console.Write(ioZNode.Value);
                }

                Console.WriteLine("\nDQ mid");
                q.RemoveAsync(five, five.Qid).FastPath().GetAwaiter();
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
                q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
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
                q.RemoveAsync(q.Tail, q.Tail.Qid).FastPath().GetAwaiter();
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
                q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
                q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
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
                    q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
                    q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
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
            }

            var _concurrentTasks = new List<Task>();

            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var rounds = concurrencyLevel;
            var mult = 1000000;
            //var rounds = 5;
            //var mult = 1000;
            var done = 0;
            var ts = Environment.TickCount;
            for (var i = 0; i < rounds; i++)
            {
                Console.Write(".");
                var i3 = i;
                var maxDiff = rounds * 4;
                var measuredConcurrency = 0;
                _concurrentTasks.Add(Task.Factory.StartNew(async () =>
                {
                    for (var j = 0; j < mult; j++)
                    {
                        Interlocked.Increment(ref measuredConcurrency);
                        try
                        {
                            //await q.PushBackAsync(i3).FastPath();//Console.WriteLine("1");
                            await q.EnqueueAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            var ts = Environment.TickCount;
                            var dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("7");
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done -dq}, {ts.ElapsedMs()}ms");

                            //await q.EnqueueAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            await q.PushBackAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("8");
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            await q.EnqueueAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("9");
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            await q.PushBackAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("10");.
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Failed... {e.Message}");
                            break;
                        }
                        finally
                        {
                            Interlocked.Increment(ref done);
                            var p = done * 100.0 / (rounds * mult);
                            if (p % 2 == 0)
                            {
                                Console.Write($"({i3}-{q.Count} {p}% {done/ts.ElapsedMs()} Kq/s)");
                            }
                            Interlocked.Decrement(ref measuredConcurrency);
                            Debug.Assert(measuredConcurrency < rounds);
                        }
                        //if(j%2000 == 0) 
                            //Console.Write($"({i3}-{q.Count})");
                    }
                    Console.Write($"({i3}-{q.Count})");
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap());
            }

            await Task.WhenAll(_concurrentTasks);

            Console.WriteLine($"count = {q.Count}, Head = {q?.Tail?.Value}, tail = {q?.Head?.Value}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds*mult*6/(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");
            
            q.Reset();
            foreach (var ioZNode in q)
            {
                Console.Write($"{ioZNode.Value},");
            }
            
            Console.ReadLine();
            throw new Exception("done");
        }

        private static async Task ZeroQTestAsync() //TODO make unit tests
        {
            await Task.Delay(1000);
            var concurrencyLevel = Environment.ProcessorCount * 2;
            IoZeroQ<int> q = new("test", concurrencyLevel * 2);

            var _concurrentTasks = new List<Task>();

            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var rounds = concurrencyLevel;
            var mult = 1000000;
            //var rounds = 5;
            //var mult = 1000;
            var done = 0;
            var ts = Environment.TickCount;
            for (var i = 0; i < rounds; i++)
            {
                Console.Write(".");
                var i3 = i;
                var maxDiff = rounds * 4;
                _concurrentTasks.Add(Task.Factory.StartNew(async () =>
                {
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {

                            //await q.PushBackAsync(i3).FastPath();//Console.WriteLine("1");
                                q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            var ts = Environment.TickCount;
                            q.TryDequeue(out var dq); 
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            
                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            q.TryDequeue(out dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            q.TryDequeue(out dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            q.TryDequeue(out dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Failed... {e.Message}");
                            break;
                        }
                        finally
                        {
                            Interlocked.Increment(ref done);
                            var p = done * 100.0 / (rounds * mult);
                            if (p % 2 == 0)
                            {
                                Console.Write($"({i3}-{q.Count} {p}% {done / ts.ElapsedMs()} Kq/s)");
                            }
                        }
                        //if(j%2000 == 0) 
                        //Console.Write($"({i3}-{q.Count})");
                    }
                    Console.Write($"({i3}-{q.Count})");
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap());
            }

            await Task.WhenAll(_concurrentTasks);

            Console.WriteLine($"count = {q.Count}, Head = {q?.Tail}, tail = {q?.Head}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");

            await q.ZeroManagedAsync<object>().FastPath();
            foreach (var ioZNode in q)
            {
                Console.Write($"{ioZNode},");
            }

            Console.ReadLine();
            throw new Exception("\ndone");
        }

        private static async Task BagTestAsync() //TODO make unit tests
        {
            await Task.Delay(1000);
            var concurrencyLevel = Environment.ProcessorCount * 2;
            IoBag<int> q = new("test", concurrencyLevel * 2);

            var _concurrentTasks = new List<Task>();

            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var rounds = concurrencyLevel;
            var mult = 1000000;
            //var rounds = 5;
            //var mult = 1000;
            var done = 0;
            var ts = Environment.TickCount;
            for (var i = 0; i < rounds; i++)
            {
                Console.Write(".");
                var i3 = i;
                var maxDiff = rounds * 4;
                _concurrentTasks.Add(Task.Factory.StartNew(async () =>
                {
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {

                            //await q.PushBackAsync(i3).FastPath();//Console.WriteLine("1");
                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            var ts = Environment.TickCount;
                            q.TryDequeue(out var dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");


                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            q.TryDequeue(out dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            q.TryDequeue(out dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");

                            q.TryEnqueue(Volatile.Read(ref done)); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            q.TryDequeue(out dq);
                            if (dq < done - maxDiff && ts.ElapsedMs() > 0)
                                Console.WriteLine($"* diff = {done - dq}, {ts.ElapsedMs()}ms");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Failed... {e.Message}");
                            break;
                        }
                        finally
                        {
                            Interlocked.Increment(ref done);
                            var p = done * 100.0 / (rounds * mult);
                            if (p % 2 == 0)
                            {
                                Console.Write($"({i3}-{q.Count} {p}% {done / ts.ElapsedMs()} Kq/s)");
                            }
                        }
                        //if(j%2000 == 0) 
                        //Console.Write($"({i3}-{q.Count})");
                    }
                    Console.Write($"({i3}-{q.Count})");
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap());
            }

            await Task.WhenAll(_concurrentTasks);

            Console.WriteLine($"count = {q.Count}, Head = {q?.Tail}, tail = {q?.Head}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds * mult * 6 / (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");

            await q.ZeroManagedAsync<object>().FastPath();
            foreach (var ioZNode in q)
            {
                Console.Write($"{ioZNode},");
            }

            Console.ReadLine();
            throw new Exception("\ndone");
        }

        /// <summary>
        /// Tests the semaphore
        /// </summary>
        private static async Task SemTestAsync() //TODO make unit tests
        {
            await Task.Delay(1000);
            var asyncTasks = new CancellationTokenSource();

            var capacity = 4;

            //.NET RUNTIME REFERENCE MUTEX FOR TESTING
            //var mutex = new IoZeroRefMut(asyncTasks.Token);
            IIoZeroSemaphoreBase<int> mutex = new IoZeroSemaphoreSlim(asyncTasks, "mutex TEST", maxBlockers: capacity, initialCount: 1, zeroAsyncMode: false, enableAutoScale: false, enableFairQ: false, enableDeadlockDetection: true);

            var releaseCount = 2;
            var waiters = 3;
            var releasers = 4;
            var disableRelease = false;
            var targetSleep = (long)0;
            var logSpam = 50000;//at least 1
            var totalReleases = int.MaxValue;

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
            uint dq1 = 0;
            uint dq2 = 0;
            uint dq3 = 0;
            var dq1_fps = 0.0;
            var dq2_fps = 0.0;
            var dq3_fps = 0.0;
            var eQ_fps = new double[10];
            var r = new Random();
            var eQ = new long[10];
            var ERR_T = 250;
            

            //TaskCreationOptions options = TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously;
            //TaskCreationOptions options = TaskCreationOptions.None;
            TaskCreationOptions options = TaskCreationOptions.DenyChildAttach;

            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var mainSW = new Stopwatch();
            const int qTimeE_ms = 16;
            var t1 = Task.Factory.StartNew(async () =>
             {
                 try
                 {
                     while (waiters>0)
                     {
                         mainSW.Restart();
                         //await Task.Delay(2500);
                         var qt = await mutex.WaitAsync().FastPath();
                         //Debug.Assert(qt.ElapsedMs() < ERR_T);
                         if (qt.ElapsedMs() < targetSleep + ERR_T)
                         {
                             
                             var tt = mainSW.ElapsedMilliseconds;
                             Interlocked.Increment(ref dq1);

                             Action a = (tt - targetSleep * targetSleepMult) switch
                             {
                                 > qTimeE_ms<<1 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                 > qTimeE_ms => () => Console.ForegroundColor = ConsoleColor.DarkYellow,
                                 > qTimeE_ms>>1 => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                                 < -qTimeE_ms << 1 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                 < -qTimeE_ms => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                                 < -qTimeE_ms>>2 => () => Console.ForegroundColor = ConsoleColor.DarkYellow,
                                 < qTimeE_ms>>2 => () => Console.ForegroundColor = ConsoleColor.Green,

                                 _ => () => Console.ForegroundColor = ConsoleColor.DarkGray,
                             };
                             a();

                             //var curTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond - 62135596800000;
                             var curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                             double delta = (curTime - startTime + 1) / 1000.0;

                             if (delta > 5)
                             {
                                 dq1_fps = (dq1 / delta);
                                 dq2_fps = (dq2 / delta);
                                 dq3_fps = (dq3 / delta);
                                 for (int i = 0; i < releasers; i++)
                                 {
                                     eQ_fps[i] = (eQ[i] / delta);
                                 }
                             }
                             
                             //Console.WriteLine($"T1:{mut.AsyncMutex}({++c}) t = {tt - targetSleep}ms, {fps.Fps(): 00.0}");
                             if (Interlocked.Increment(ref c) % logSpam == 0)
                             {
                                 var totalEq = eQ.Aggregate((u, u1) => u + u1);
                                 Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] T1:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{dq1_fps + dq2_fps + dq3_fps: 0}, ({dq1_fps: 0}, {dq2_fps: 0}, {dq3_fps: 0})], [{totalEq/delta: 0.0} ({eQ_fps[0]: 0}, {eQ_fps[1]: 0}, {eQ_fps[2]: 0}, {eQ_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, S = {mutex.ReadyCount}, DQ DELTA = {(int)(dq1 + dq2 + dq3) - (int)totalEq} (+ means release is not working)");
                                 Console.ResetColor();
                             }
                              
                             // if(r.Next()%2 != 0)
                             //     await Task.Delay(1).ConfigureAwait(ZC);
                         }
                         else
                         {
                             Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] T1: {qt.ElapsedMs()}ms - {qt}");
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

                       mainSW.Restart();
                        var qt = await mutex.WaitAsync().FastPath();
                        //Debug.Assert(qt.ElapsedMs() < ERR_T);
                        if (qt.ElapsedMs() < targetSleep + ERR_T)
                        { 
                            var tt = mainSW.ElapsedMilliseconds;
                            Interlocked.Increment(ref dq2);
                            
                            Action a = (tt - targetSleep * targetSleepMult) switch
                            {
                                > qTimeE_ms << 1 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                > qTimeE_ms => () => Console.ForegroundColor = ConsoleColor.DarkYellow,
                                > qTimeE_ms >> 1 => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                                < -qTimeE_ms << 1 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                < -qTimeE_ms => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                                < -qTimeE_ms >> 2 => () => Console.ForegroundColor = ConsoleColor.DarkYellow,
                                < qTimeE_ms >> 2 => () => Console.ForegroundColor = ConsoleColor.Green,

                                _ => () => Console.ForegroundColor = ConsoleColor.DarkGray,
                            };
                            a();
                            
                            var curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            double delta = (curTime - startTime + 1) / 1000.0;
                            
                            if (delta > 5)
                            {
                                dq1_fps = (dq1 / delta);
                                dq2_fps = (dq2 / delta);
                                dq3_fps = (dq3 / delta);
                                for (int i = 0; i < releasers; i++)
                                {
                                    eQ_fps[i] = (eQ[i] / delta);
                                }
                            }
                             
                            curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            delta = (curTime - startTime + 1)/1000.0;
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                            {
                                var totalEq = eQ.Aggregate((u, u1) => u + u1);
                                Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] T2:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{dq1_fps + dq2_fps + dq3_fps: 0}, ({dq1_fps: 0}, {dq2_fps: 0}, {dq3_fps: 0})], [{totalEq/delta: 0.0} ({eQ_fps[0]: 0}, {eQ_fps[1]: 0}, {eQ_fps[2]: 0}, {eQ_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, S = {mutex.ReadyCount}, D = {(int)(dq1 + dq2 + dq3) - (int)totalEq}");
                                Console.ResetColor();
                            }
                             
                            // if(r.Next()%2 != 0)
                            //     await Task.Delay(1).ConfigureAwait(ZC);
                        }
                        else
                        {
                            Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] T2: {qt.ElapsedMs()}ms - {qt}");
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

                       mainSW.Restart();
                        var qt = await mutex.WaitAsync().FastPath();
                        //Debug.Assert(qt.ElapsedMs() < ERR_T);
                        if (qt.ElapsedMs() < targetSleep + ERR_T)
                        {
                           var tt = mainSW.ElapsedMilliseconds;
                           Interlocked.Increment(ref dq3);
           
                           Action a = (tt - targetSleep * targetSleepMult) switch
                           {
                               > qTimeE_ms << 1 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                               > qTimeE_ms => () => Console.ForegroundColor = ConsoleColor.DarkYellow,
                               > qTimeE_ms >> 1 => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                               < -qTimeE_ms << 1 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                               < -qTimeE_ms => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                               < -qTimeE_ms >> 2 => () => Console.ForegroundColor = ConsoleColor.DarkYellow,
                               < qTimeE_ms >> 2 => () => Console.ForegroundColor = ConsoleColor.Green,

                               _ => () => Console.ForegroundColor = ConsoleColor.DarkGray,
                           };
                           a();
                            
                            var curTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            double delta = (curTime - startTime + 1)/1000.0;
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                            {
                                var totalDq = eQ.Aggregate((u, u1) => u + u1);
                                Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] T3:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{dq1_fps + dq2_fps + dq3_fps: 0}, ({dq1_fps: 0}, {dq2_fps: 0}, {dq3_fps: 0})], [{totalDq/delta: 0.0} ({eQ_fps[0]: 0}, {eQ_fps[1]: 0}, {eQ_fps[2]: 0}, {eQ_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, S = {mutex.ReadyCount}, D = {(int)(dq1 + dq2 + dq3) - (int)totalDq}");
                                Console.ResetColor();
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] T3: {qt.ElapsedMs()}ms - {qt}");
                            //break;
                        }

                   }
               }
               catch (Exception e)
               {
                   Console.WriteLine($"[[3]]:{e}");
                   throw;
               }
           }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Current);
            
            if(!disableRelease)
                for (int i = 0; i < releasers; i++)
                {
                    var i1 = i;
                    var r3 = Task.Factory.StartNew(async () =>
                    {
                        try
                        {
                            var released = 0;
                            Console.WriteLine($"releaser: [{i1}]");
                            while (releasers > 0)
                            {
                                try
                                {
                                    if (targetSleep > 0)
                                        await Task.Delay((int)targetSleep, asyncTasks.Token);
                                    
                                    if (Interlocked.Decrement(ref totalReleases) >= 0 && (released = mutex.Release(Environment.TickCount, releaseCount)) > 0)
                                    {
                                        //Interlocked.Add(ref semCount, curCount);
                                        Interlocked.Increment(ref semPollCount);
                                        Interlocked.Add(ref eQ[i1], released);
                                    }
                                    else
                                    {
                                        //await Task.Yield();
                                        await Task.Delay(1, asyncTasks.Token);
                                        if (Interlocked.Decrement(ref totalReleases) <= 0)
                                            break;
                                    }
                                    //if(i1 != 0)
                                    //    Console.WriteLine($"exit! {i1}");
                                }
                                catch (InvalidOperationException e)
                                {

                                    Console.WriteLine($"Failed... {e.Message}");
                                }
                                catch (TaskCanceledException)
                                {
                                    return;
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine($"Failed... {e.Message}");
                                }
                            }

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"3:{e}");
                            throw;
                        }
                    }, asyncTasks.Token, options, TaskScheduler.Current);
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
                        await n.DisposeAsync(null,"MAIN TEARDOWN");
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
            //await AutoPeeringEventService.ClearAsync().FastPath();
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

//                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.DisposeAsync(new IoNanoprobe("process was exited"));
//                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.DisposeAsync(new IoNanoprobe("ctrl + c, process was killed"));
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
//                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.DisposeAsync(new IoNanoprobe("process was exited"));
//                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.DisposeAsync(new IoNanoprobe("ctrl + c, process was killed"));
//#pragma warning restore 4014
//                tangleNodeTask.AsTask().Wait();
//            }
//        }

        private static CcCollective CoCoon(CcDesignation ccDesignation, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, IEnumerable<string> bootStrapAddress, bool zeroDrone = false)
        {

            var cocoon = new CcCollective(ccDesignation,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                3, 2, 2, 1, zeroDrone);

            _nodes.Add(cocoon);
            return cocoon;
        }

        private static void StartCocoon(CcCollective cocoon)
        {
            //IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
            //{
            //    var cocoon = (CcCollective)state;
            //    //await cocoon.StartAsync<object>().FastPath();
            //    await cocoon.StartAsync(c =>
            //    {
            //        c.Emit();
            //        return default;
            //    }, cocoon, TaskScheduler.Default).FastPath();

            //}, cocoon);
            _= Task.Factory.StartNew(static async state =>
            {
                var cocoon = (CcCollective)state;
                //await cocoon.StartAsync<object>().FastPath();
                await cocoon.StartAsync(c =>
                {
                    c.Emit();
                    return default;
                }, cocoon, TaskScheduler.Default).FastPath();
            }, cocoon, CancellationToken.None,TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }
    }
}
