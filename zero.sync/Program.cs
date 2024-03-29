﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
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
using zero.core.network.tools;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.patterns.semaphore;
using zero.core.patterns.semaphore.core;
using zero.core.runtime.scheduler;
using zero.core.runtime.threadpool;
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
                        builder.AddFilter(level => level > LogLevel.Trace);
                        builder.ClearProviders();
                        builder.SetMinimumLevel(LogLevel.Trace);
                    });

                });

        static void Bootstrap(out ConcurrentBag<CcCollective> concurrentBag, int total, int portOffset = 20000, int localPort = -1, int[] remotePort = null)
        {
            var wantedDrones = 4;
            var maxDrones = 8;
            var maxAdjuncts = 16;

            var oldBoot = localPort == -1;
            var queens = false;

            concurrentBag = new ConcurrentBag<CcCollective>();
            if (oldBoot)
            {
                if (queens)
                // ReSharper disable once HeuristicUnreachableCode
                {
                    CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1234}",
                        $"udp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                        new string[]
                        {
                                $"udp://127.0.0.1:{1235}",
                                $"udp://127.0.0.1:{1236}",
                                $"udp://127.0.0.1:{1237}"
                        }.ToList(), queens);

                    CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1235}",
                        $"udp://127.0.0.1:{1235}",
                        $"udp://127.0.0.1:{1235}",
                        new[]
                        {
                                $"udp://127.0.0.1:{1234}",
                                $"udp://127.0.0.1:{1236}",
                                $"udp://127.0.0.1:{1237}"
                        }.ToList(), queens);

                    CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1236}",
                        $"udp://127.0.0.1:{1236}", $"udp://127.0.0.1:{1236}",
                        new[]
                        {
                                $"udp://127.0.0.1:{1235}",
                                $"udp://127.0.0.1:{1234}",
                                $"udp://127.0.0.1:{1237}"
                        }.ToList(), queens);

                    CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1237}",
                        $"udp://127.0.0.1:{1237}",
                        $"udp://127.0.0.1:{1237}",
                        new[]
                        {
                                $"udp://127.0.0.1:{1234}",
                                $"udp://127.0.0.1:{1235}",
                                $"udp://127.0.0.1:{1236}"
                        }.ToList(), queens);
                }
                else
                {
                    //StartCocoon(CoCoon(CcDesignation.Generate(true), $"tcp://127.0.0.1:{1234}",
                    //    $"udp://127.0.0.1:{1234}",
                    //    $"tcp://127.0.0.1:{1234}", $"udp://127.0.0.1:{1234}",
                    //    new string[]
                    //    {
                    //            $"udp://127.0.0.1:{1235}",
                    //        //$"udp://127.0.0.1:{1236}",
                    //        //$"udp://127.0.0.1:{1237}"
                    //    }.ToList(), false));

                    //StartCocoon(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{1235}",
                    //    $"udp://127.0.0.1:{1235}", $"tcp://127.0.0.1:{1235}",
                    //    $"udp://127.0.0.1:{1235}",
                    //    new string[]
                    //    {
                    //            $"udp://127.0.0.1:{1234}",
                    //        //$"udp://127.0.0.1:{1236}",
                    //        //$"udp://127.0.0.1:{1237}"
                    //    }.ToList(), false));

                    StartCocoon(CoCoon(CcDesignation.Generate(true), $"tcp://192.168.88.253:{1234}",
                        $"udp://192.168.88.253:{1234}", $"udp://192.168.88.253:{1234}",
                        new string[]
                        {
                            $"udp://192.168.88.253:{1235}",
                            //$"udp://127.0.0.1:{1236}",
                            //$"udp://127.0.0.1:{1237}"
                        }.ToList(), false));

                    StartCocoon(CoCoon(CcDesignation.Generate(), $"tcp://192.168.88.253:{1235}",
                        $"udp://192.168.88.253:{1235}",
                        $"udp:/192.168.88.253:{1235}",
                        new string[]
                        {
                            $"udp://192.168.88.253:{1234}",
                            //$"udp://127.0.0.1:{1236}",
                            //$"udp://127.0.0.1:{1237}"
                        }.ToList(), false));
                }
            }
            else
            {
                //var count = remotePort.Count(p => p > 0);
                //StartCocoon(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{localPort}",
                //    $"udp://127.0.0.1:{localPort}",
                //    $"tcp://127.0.0.1:{localPort}", $"udp://127.0.0.1:{localPort}",
                //    //remotePort.Take(count).Select(p => $"udp://10.0.0.6:{p}, udp://10.0.0.5:{p}").ToList(), false));
                //    remotePort.Take(count).Select(p => $"udp://10.0.0.6:{p}, udp://10.0.0.5:{p}").ToList(), false));

                var count = remotePort.Count(p => p > 0);
                StartCocoon(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{localPort}",
                    $"udp://127.0.0.1:{localPort}", $"udp://127.0.0.1:{localPort}",
                    remotePort.Take(count).Select(p => $"udp://127.0.0.1:{p}").ToList(), false));

                    Console.WriteLine($"starting udp://127.0.0.1:{localPort} -> {string.Join(", ", remotePort.Take(count).Select(port => $"udp://127.0.0.1:{port}"))}");
                return;
            }

            Thread.Sleep(2000);
            int basep = 3234 + portOffset;

            Thread.Sleep(2000);

            for (var i = 0; i < total; i++)
            {
                string extra = i == 0 ? $"udp://127.0.0.1:{1235}" : $"udp://127.0.0.1:{basep + i - 1}";

                concurrentBag.Add(CoCoon(CcDesignation.Generate(), $"tcp://127.0.0.1:{basep + i}",
                    $"udp://127.0.0.1:{basep + i}",
                    $"udp://127.0.0.1:{basep + i}",
                    new[]
                    {
                            extra,
                            $"udp://127.0.0.1:{basep + (i + 1 + Random.Shared.Next(0, i))%total}"
                    }.ToList()));

                if (concurrentBag.Count % 10 == 0)
                    Console.WriteLine($"Spawned {concurrentBag.Count}/{total}");
            }

            var bag = concurrentBag;
            _ = Task.Factory.StartNew(async () =>
            {
                Console.WriteLine($"Starting auto peering...  {bag.Count}");
                var c = 0;
                var rateLimit = bag.Count * 20;
                var injectionCount = Math.Max(1, bag.Count / 20);
                var rampDelay = 100;
                foreach (var cocoon in bag.OrderBy(e => e.Serial))
                {
                    await Task.Delay(rampDelay).ConfigureAwait(false);
                    if (rampDelay > 50)
                        rampDelay -= 1;

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

                        await Task.Delay(rateLimit -= 1).ConfigureAwait(false);

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
                                            $"t = {ioCcNode.TotalConnections} ({ioCcNode.IngressCount},{ioCcNode.EgressCount}), [{ioCcNode.Description}] -> {drone.Description} ][ {drone.Adjunct.MetaDesc}, uptime = {drone.UpTime.ElapsedUtcMs():0.0}s");
                                    }
                                }

                                ooutBound += e;
                                oinBound += i;
                                if(ioCcNode.Hub == null)
                                    continue;
                                
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
                                    $"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / ((double)_nodes.Count * wantedDrones) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt}), con = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1.0):0.0}ms,  [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
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
                                    $"({outBound},{inBound}), ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones}, p = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / (double)(_nodes.Count * wantedDrones) * 100:0.00}%, t = {TimeSpan.FromSeconds(uptime / (double)uptimeCount).TotalHours:0.000}h, T = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, w = {-wt + maxwt}, ports = {-cpt + maxcpt}, connect time = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1.0):0.0}ms, [iKo/s = {fps / 1000.0:0.000}, i = {localOps / 1000000.0:0.000} Mil], [tKo/s = {tfps / 1000.0:0.000}, t = {totalOps / 1000000.0:0.000} Mil]");
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
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);
        }
        static void CoreTest()
        {
            //var t2 = Task.Factory.StartNew(async () =>
            //{
            //    await Task.Delay(1000);
            //    Console.WriteLine("TEST TASK STARTED [OK]");
            //}, CancellationToken.None, TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault);

            //------------------------ TEST-----------------------------------
            var j = new JoinableTaskFactory(new JoinableTaskContext());
            {
                var t = j.RunAsync(async () =>
                { 
                    //await SemTestAsync();
                    await QueueTestAsync();
                    //await ZeroQTestAsync();
                    //await BagTestAsync();
                });
                t.Join();
            }
            Console.ReadLine();
            //------------------------ TEST-----------------------------------
        }


        static void PrimePeriodicTimer()
        {
            IoTimer.Make(make: static (delta, signal, token) =>
            {
                _ = Task.Factory.StartNew(static async state =>
                {
                    var (delta, signal, token) = (ValueTuple<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken>)state;

                    try
                    {
                        

                        signal.RunContinuationsAsynchronouslyAlways = true;
                        var p = new PeriodicTimer(delta);
                        while (!token.IsCancellationRequested)
                        {
                            if (await p.WaitForNextTickAsync(token).FastPath())
                                signal.SetResult(Environment.TickCount);
                            else
                                signal.SetException(new OperationCanceledException());
                        }
                    }
                    catch (TaskCanceledException){}
                    catch (OperationCanceledException){}
                    catch (Exception e)when(token.CanBeCanceled) 
                    {
                            LogManager.GetCurrentClassLogger().Error(e, $"{nameof(IoTimer)}:");
                    }

                }, (delta, signal, token), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

//                var t = new Thread(static async state =>
//                {
//                    var (delta, signal, token) = (ValueTuple<TimeSpan, IIoManualResetValueTaskSourceCore<int>, CancellationToken>)state;

//                    signal.RunContinuationsAsynchronouslyAlways = true;
//                    var p = new PeriodicTimer(delta);
//                    while (!token.IsCancellationRequested)
//                    {
//                        if (await p.WaitForNextTickAsync(token).FastPath())
//                            signal.SetResult(Environment.TickCount);
//                        else
//                            signal.SetException(new OperationCanceledException());
//                    }
//                });
//                t.Start((delta, signal, token));
            });
        }

        static IHost _host = null;

        static void StartGRPC()
        {
            //BOOSTRAP GRPC
            if (AutoPeeringEventService.Operational)
            {
                var grpc = Task.Factory.StartNew(() =>
                {
                    _host = CreateHostBuilder(AutoPeeringEventService.Port = 27021).Build();
                    _host.Run();
                }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
            }
        }

        /// <summary>
        /// Cluster test mode
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {

            Console.WriteLine($"zero ({Environment.OSVersion}: {Environment.MachineName} - dotnet v{Environment.Version}, CPUs = {Environment.ProcessorCount})");

            var prime = IoZeroScheduler.ZeroDefault; //TODO: how do we prime the scheduler automagically?
            if (prime.Id > 1)
                Console.WriteLine("using IoZeroScheduler");

            LogManager.Setup().LoadConfigurationFromFile("nlog.config");
            Console.WriteLine("Type 'help' for a list of test commands");

            //Tune dotnet for large tests
            //ThreadPool.GetMinThreads(out var wt, out var cp);
            //ThreadPool.SetMinThreads(wt * 3, cp * 2);

            PrimePeriodicTimer();
            IoThreadPoolHooks<object>.Init(ThreadPool.UnsafeQueueUserWorkItem);

            //run tests
            //CoreTest();

            //IoThreadPoolHooks.Default = new IoThreadPoolHooks();

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

            ConcurrentBag<CcCollective> tasks = new ();

            if (localPort != -1)
            {
                LogManager.Configuration.Variables["zeroLogLevel"] = "trace";
                LogManager.ReconfigExistingLoggers();
                Bootstrap(out tasks, total, 0, localPort, remotePort);
            }

            //Console.WriteLine($"local = {localPort}, remote = {remotePort}");

            StartGRPC();


            //Continue to print some stats while tests are running... 
            long C = 0;

            string line;
            var ts = Environment.TickCount;
            var tsOrig = Environment.TickCount;
            var cOrig = ThreadPool.CompletedWorkItemCount;
            var c = ThreadPool.CompletedWorkItemCount;
            

            var LS = Environment.TickCount;
            var LSOrig = Environment.TickCount;
            var LOrig = IoZeroScheduler.Zero.AsyncForkCount;
            var LC = IoZeroScheduler.Zero.AsyncForkCount;

            var QS = Environment.TickCount;
            var QSOrig = Environment.TickCount;
            var QOrig = IoZeroScheduler.Zero.TaskDequeueCount;
            var QC = IoZeroScheduler.Zero.TaskDequeueCount;

            var AS = Environment.TickCount;
            var AC = IoZeroScheduler.Zero.AsyncTaskWithContextCount;

            var FS = Environment.TickCount;
            var FC = IoZeroScheduler.Zero.AsyncZeroCount;

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
                    if (line.StartsWith("pid"))
                    {
                        Console.Write($"pid = {Environment.ProcessId}\n");
                    }


                    if (line.Contains("boot"))
                    {
                        var tokens = line.Split(' ');
                        if (tokens.Length > 1)
                            int.TryParse(tokens[1], out total);

                        Bootstrap(out tasks, total);
                    }

                    if (line.Contains("add"))
                    {
                        var tokens = line.Split(' ');
                        if (tokens.Length > 1)
                        {
                            int.TryParse(tokens[1], out localPort);

                            if (!string.IsNullOrEmpty(tokens[1]))
                            {
                                var l = 1;
                                while (l < tokens.Length)
                                {
                                    int.TryParse(tokens[l], out remotePort[l - 1]);
                                    l++;
                                }
                            }

                            Bootstrap(out tasks, total, 0, localPort, remotePort);
                        }
                    }

                    if (line == "gc")
                    {
                        var pinned = GC.GetGCMemoryInfo(GCKind.Any).PinnedObjectsCount;
                        for(int i = 0;i < 10; i++)
                            GC.Collect(GC.MaxGeneration);
                        Console.WriteLine($"Pinned was = {pinned}, now {GC.GetGCMemoryInfo(GCKind.Any).PinnedObjectsCount}");
                    }
                
                    if (line == "quit")
                        break;

                    if (line == "t")
                    {
                        Console.WriteLine($"load = {IoZeroScheduler.Zero.Load}({IoZeroScheduler.Zero.LoadFactor * 100:0.0}%), q time = {ThreadPool.PendingWorkItemCount / ((ThreadPool.CompletedWorkItemCount - c) / (double)ts.ElapsedMs()):0}ms, threads = {ThreadPool.ThreadCount}({IoZeroScheduler.Zero.ThreadCount}), p = {ThreadPool.PendingWorkItemCount}({IoZeroScheduler.Zero.Wait}), t = {ThreadPool.CompletedWorkItemCount}, {(ThreadPool.CompletedWorkItemCount - cOrig) / (double)tsOrig.ElapsedMsToSec():0.0} ops, c = {ThreadPool.CompletedWorkItemCount-c}, {(ThreadPool.CompletedWorkItemCount-c)/(double)ts.ElapsedMsToSec():0.0} tps");
                        ts = Environment.TickCount;
                        c = ThreadPool.CompletedWorkItemCount;
                    }

                    if (line == "L")
                    {
                        Console.WriteLine($"[{IoZeroScheduler.Zero.Load}/{IoZeroScheduler.Zero.AsyncZeroLoad}/{IoZeroScheduler.Zero.ForkLoad}/{IoZeroScheduler.Zero.AsyncFallBackLoad}/{IoZeroScheduler.Zero.Capacity}] load = {IoZeroScheduler.Zero.Load/IoZeroScheduler.Zero.Capacity * 100:0.0}%, az time = {IoZeroScheduler.Zero.AqTime:0.000}ms, aq time = {IoZeroScheduler.Zero.AqTime:0.000}ms, sem = {IoZeroScheduler.Zero.Wait}/{IoZeroScheduler.Zero.Idle} ({IoZeroScheduler.Zero.Rate:0.0} s/s); {(IoZeroScheduler.Zero.AsyncForkCount - LC) / (double)LS.ElapsedMsToSec():0.0} o/s, {(IoZeroScheduler.Zero.TaskDequeueCount - QC) / (double)QS.ElapsedMsToSec():0.0} z/s ({IoZeroScheduler.Zero.TaskDequeueCount - QC})[{IoZeroScheduler.Zero.TaskDequeueCount}]; {(IoZeroScheduler.Zero.AsyncTaskWithContextCount - AC) / (double)AS.ElapsedMsToSec():0.0} a/s ({IoZeroScheduler.Zero.AsyncTaskWithContextCount - AC})[{IoZeroScheduler.Zero.AsyncTaskWithContextCount}]; {(IoZeroScheduler.Zero.AsyncZeroCount - FC) / (double)FS.ElapsedMsToSec():0.0} b/s ({IoZeroScheduler.Zero.AsyncZeroCount - FC})[{IoZeroScheduler.Zero.AsyncZeroCount}];");
                        QS = Environment.TickCount;
                        AS = Environment.TickCount;
                        LS = Environment.TickCount;
                        FS = Environment.TickCount;
                        QC = IoZeroScheduler.Zero.TaskDequeueCount;
                        LC = IoZeroScheduler.Zero.AsyncForkCount;
                        AC = IoZeroScheduler.Zero.AsyncTaskWithContextCount;
                        FC = IoZeroScheduler.Zero.AsyncZeroCount;
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
                        IoZeroScheduler.Zero.QueueAsyncFunction(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(1).FastPath());
                        if (_nodes.Count >= 4)
                        {
                            IoZeroScheduler.Zero.QueueAsyncFunction(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(10).FastPath());
                            IoZeroScheduler.Zero.QueueAsyncFunction(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(20).FastPath());
                            IoZeroScheduler.Zero.QueueAsyncFunction(async () => await n[Random.Shared.Next(0, n.Length - 1)].BootAsync(30).FastPath());
                        }

                        Console.WriteLine("ZERO CORE test...");
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
                                IoZeroScheduler.Zero.QueueAsyncFunction(async () =>
                                {
                                    await Task.WhenAll(gossipTasks.ToArray());
                                    gossipTasks.Clear();
                                    await AutoPeeringEventService.ClearAsync().FastPath();
                                });
                            }

                            long v = 1;
                            _rampDelay = 100;
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
                            
                                Console.WriteLine($"zero liveness at {nodes.Count/((double)_nodes.Count - 4)*100:0.00}%, {nodes.Count}/{_nodes.Count-4}, min/max = [{min},{max}], ave = {ave:0.0}, err = {err:0.0}, r = {r}");
                            }
                            catch (Exception) { }
                        }
                    }

                    if (line.StartsWith("stream"))
                    {
                        IoZeroScheduler.Zero.QueueAsyncFunction(async () => await AutoPeeringEventService.ToggleActiveAsync());
                        
                        Console.WriteLine($"event stream = {(AutoPeeringEventService.Operational? "On":"Off")}");
                    }

                    if (line.StartsWith("zero"))
                    {
                        IoZeroScheduler.Zero.QueueAsyncFunction(async () =>
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

                    if (line.StartsWith("help"))
                    {
                        Console.WriteLine("\n               -- ZERO CORE TEST SUITE --\n");
                        Console.WriteLine("test usage:\n");
                        Console.WriteLine("boot <n>            - boot a new cluster with 'n' nodes");
                        Console.WriteLine("add local [remotes] - Add a new node at local port that boostraps from a list of remote ports all on 0.0.0.0");
                        Console.WriteLine("loadTest            - start cluster Lamport timestamp test (preferred after cluster bootstrap is complete or near 100%)");
                        Console.WriteLine("zero                - destroys the current cluster");
                        Console.WriteLine("log [level]         - set log level to 'info', 'debug', 'trace'");
                        Console.WriteLine("gc                  - Force a full generation garbage collect");
                        Console.WriteLine("L                   - Display zero scheduler stats");
                        Console.WriteLine("t                   - Display runtime scheduler stats");
                        Console.WriteLine("pid                 - Display process id");
                        Console.WriteLine("gateway info        - Display ip gateway info");
                        Console.WriteLine("quit                - quit");
                    }

                    if (line == "gw")
                    {
                        Console.WriteLine(IoPing.Info());
                        Console.WriteLine(IoPing.Description());
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

            if (_host != null)
            {
                var s = _host.StopAsync();
                _host = null;
            }
            
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
            var concurrencyLevel = Environment.ProcessorCount * 2;
            //var concurrencyLevel = 4;
            IoQueue<int> q = new("test", 4096, concurrencyLevel, IoQueue<int>.Mode.BackPressure | IoQueue<int>.Mode.Pressure);
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
                _ = q.RemoveAsync(five, five.Qid).FastPath().GetAwaiter();
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
                _ = q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
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
                _ = q.DequeueAsync().FastPath().GetAwaiter();
                _ = q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
                _ = q.RemoveAsync(q.Head, q.Head.Qid).FastPath().GetAwaiter();
                _ = q.DequeueAsync().FastPath().GetAwaiter();

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
                var maxDiff = 10000;
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
                            if (dq < done - maxDiff * 100)
                                Console.Write($" -> * diff = {done -dq}, {ts.ElapsedMs()}ms");

                            //await q.EnqueueAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            await q.PushBackAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("8");
                            if (dq < done - maxDiff * 100)
                                Console.Write($" -> * diff = {done - dq}, {ts.ElapsedMs()}ms");

                            await q.EnqueueAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("9");
                            if (dq < done - maxDiff * 100)
                                Console.Write($" -> * diff = {done - dq}, {ts.ElapsedMs()}ms");

                            await q.PushBackAsync(Volatile.Read(ref done)).FastPath(); //Console.WriteLine("2");
                            ts = Environment.TickCount;
                            dq = await q.DequeueAsync().FastPath(); //Console.WriteLine("10");.
                            if (dq < done - maxDiff * 100)
                                Console.Write($" -> * diff = {done - dq}, {ts.ElapsedMs()}ms");
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
                                Console.Write($"({i3}-{q.Count} {p}% {maxDiff = done/ts.ElapsedMs()} Kq/s)");
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

            var concurrentTasks = new List<Task>();

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
                concurrentTasks.Add(Task.Factory.StartNew(() =>
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
                    return Task.CompletedTask;
                }, new CancellationToken(), TaskCreationOptions.DenyChildAttach, IoZeroScheduler.ZeroDefault).Unwrap());
            }

            await Task.WhenAll(concurrentTasks);

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
            //var concurrencyLevel = Environment.ProcessorCount * 2;
            var concurrencyLevel = 2;
            IoBag<int> q = new("test", concurrencyLevel);

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
                _concurrentTasks.Add(Task.Factory.StartNew(() =>
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
                    return Task.CompletedTask;
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
            IIoZeroSemaphoreBase<int> mutex = new IoZeroSemaphoreSlim(asyncTasks, "mutex TEST", maxBlockers: capacity, initialCount: 0, zeroAsyncMode: false);

            //var releaseCount = 2;
            //var waiters = 3;
            //var releasers = 4;
            //var disableRelease = false;
            //var targetSleep = (long)0;
            //var logSpam = 50000;//at least 1
            //var totalReleases = int.MaxValue;

            var releaseCount = 2;
            var waiters = 3;
            var releasers = 4;
            var disableRelease = false;
            var targetSleep = (long)0;
            var logSpam = 30000;//at least 1
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
                         Interlocked.Increment(ref dq1);
                         if (qt.ElapsedMs() < targetSleep + ERR_T)
                         {
                             
                             var tt = mainSW.ElapsedMilliseconds;
                             

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
                                 Console.WriteLine($"[{Environment.CurrentManagedThreadId}] T1:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{dq1_fps + dq2_fps + dq3_fps: 0}, ({dq1_fps: 0}, {dq2_fps: 0}, {dq3_fps: 0})], [{totalEq/delta: 0.0} ({eQ_fps[0]: 0}, {eQ_fps[1]: 0}, {eQ_fps[2]: 0}, {eQ_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, R = {mutex.ReadyCount}, W = {mutex.WaitCount}, D = {(int)(dq1 + dq2 + dq3) - (int)totalEq}");
                                 Console.ResetColor();
                             }
                              
                             // if(r.Next()%2 != 0)
                             //     await Task.Delay(1).ConfigureAwait(ZC);
                         }
                         else
                         {
                             Console.WriteLine($"[{Environment.CurrentManagedThreadId}] T1: {qt.ElapsedMs()}ms");
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
                       Interlocked.Increment(ref dq2);
                       if (qt.ElapsedMs() < targetSleep + ERR_T)
                       { 
                            var tt = mainSW.ElapsedMilliseconds;
                            
                            
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
                                Console.WriteLine($"[{Environment.CurrentManagedThreadId}] T2:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{dq1_fps + dq2_fps + dq3_fps: 0}, ({dq1_fps: 0}, {dq2_fps: 0}, {dq3_fps: 0})], [{totalEq/delta: 0.0} ({eQ_fps[0]: 0}, {eQ_fps[1]: 0}, {eQ_fps[2]: 0}, {eQ_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, R = {mutex.ReadyCount}, W = {mutex.WaitCount}, D = {(int)(dq1 + dq2 + dq3) - (int)totalEq}");
                                Console.ResetColor();
                            }
                             
                            // if(r.Next()%2 != 0)
                            //     await Task.Delay(1).ConfigureAwait(ZC);
                       }
                       else
                       {
                           Console.WriteLine($"[{Environment.CurrentManagedThreadId}] T2: {qt.ElapsedMs()}ms");
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
                       Interlocked.Increment(ref dq3);
                       if (qt.ElapsedMs() < targetSleep + ERR_T)
                       {
                           var tt = mainSW.ElapsedMilliseconds;
                           
           
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
                                var totalEq = eQ.Aggregate((u, u1) => u + u1);
                                Console.WriteLine($"[{Environment.CurrentManagedThreadId}] T3:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{dq1_fps + dq2_fps + dq3_fps: 0}, ({dq1_fps: 0}, {dq2_fps: 0}, {dq3_fps: 0})], [{totalEq/delta: 0.0} ({eQ_fps[0]: 0}, {eQ_fps[1]: 0}, {eQ_fps[2]: 0}, {eQ_fps[3]: 0})], s = {semCount / (double)(semPollCount+1):0.0}, R = {mutex.ReadyCount}, W = {mutex.WaitCount}, D = {(int)(dq1 + dq2 + dq3) - (int)totalEq}");
                                Console.ResetColor();
                            }
                        }
                        else
                        {
                           Console.WriteLine($"[{Environment.CurrentManagedThreadId}] T3: {qt.ElapsedMs()}ms");
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
            await asyncTasks.CancelAsync();
            Console.ReadLine();
            Console.WriteLine("Done");
            Console.ReadLine();
        }

        private static ValueTask ZeroAsync(int total)
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
            
            return ValueTask.CompletedTask;
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

        private static CcCollective CoCoon(CcDesignation ccDesignation, string gossipAddress, string peerAddress, string extAddress, IEnumerable<string> bootStrapAddress, bool zeroDrone = false)
        {

            var cocoon = new CcCollective(ccDesignation,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                3, 2, 2, 1, zeroDrone);

            _nodes.Add(cocoon);
            return cocoon;
        }

        private static void StartCocoon(CcCollective cocoon)
        {
            //IoZeroScheduler.Zero.QueueAsyncFunction(static async state =>
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
