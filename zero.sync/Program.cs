﻿using System;
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
            //QueueTest();
            
            LogManager.LoadConfiguration("nlog.config");
            var portOffset = -2000;
            
#if DEBUG
            portOffset = 0;
#endif
            IHost host = null;
            var grpc = Task.Factory.StartNew(() =>
            {
                host = CreateHostBuilder(args).Build();
                host.Run();
            }, TaskCreationOptions.LongRunning);


            var random = new Random((int)DateTime.Now.Ticks);
            //Tangle("tcp://192.168.1.2:15600");
            var total = 300;
            var maxDrones = 8;
            var maxAdjuncts = 16;
            var tasks = new ConcurrentBag<Task<CcCollective>>
            {
                CoCoonAsync(CcDesignation.Generate(true), $"tcp://127.0.0.1:{14667}", $"udp://127.0.0.1:{14627}",
                    $"tcp://127.0.0.1:{11667}", $"udp://127.0.0.1:{14627}", new[] {$"udp://127.0.0.1:{14626}"}.ToList(),
                    0),
                CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15670 + portOffset}",
                    $"udp://127.0.0.1:{15630 + portOffset}", $"tcp://127.0.0.1:{11667 + portOffset}",
                    $"udp://127.0.0.1:{15630 + portOffset}",
                    new[]
                    {
                        $"udp://127.0.0.1:{14627}", $"udp://127.0.0.1:{14627 + portOffset}",
                        $"udp://127.0.0.1:{15631 + portOffset}"
                    }.ToList(), 1)
            };

            //tasks.Add(CoCoonAsync(CcIdentity.Generate(true), $"tcp://127.0.0.1:{14667 + portOffset}", $"udp://127.0.0.1:{14627 + portOffset}", $"tcp://127.0.0.1:{11667 + portOffset}", $"udp://127.0.0.1:{14627 + portOffset}", new[] { $"udp://127.0.0.1:{14626 + portOffset}", $"udp://127.0.0.1:{14626}" }.ToList(), 0));


            for (var i = 2; i < total; i++)
            {
                //tasks.Add(CoCoonAsync(CcIdentity.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", Enumerable.Range(0, 16).Select(i => $"udp://127.0.0.1:{15629 + portOffset + random.Next(total - 1)/* % (total/6 + 1)*/}").ToList(), i));
                //tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", new[] { $"udp://127.0.0.1:{15629 + portOffset + i - 2}", $"udp://127.0.0.1:{15629 + portOffset + (total - i + 2) % (total - 2)}", $"udp://127.0.0.1:{15629 + portOffset + Math.Abs(total/2 - i + 2)%(total - 2)}", $"udp://127.0.0.1:{15629 + portOffset + (total / 2 + i - 2) % (total - 2)}", $"udp://127.0.0.1:{15629 + portOffset + random.Next(total - 2)}" }.ToList(), i));
                //tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", new[] { $"udp://127.0.0.1:{14626}", $"udp://127.0.0.1:{14627}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(16)}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(16)}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(i)}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(i)}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(i)}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(i)}", $"udp://127.0.0.1:{15630 + portOffset + random.Next(i)}" }.ToList(), i));
                tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", new[] { $"udp://127.0.0.1:{15629 + portOffset + i - 1}", $"udp://127.0.0.1:{15629 + portOffset + (i + 8) % total}" }.ToList(), i));
                if (tasks.Count % 10 == 0)
                    Console.WriteLine($"Spawned {tasks.Count}/{total}...");
            }

            //Console.WriteLine("Prepping...");
            //Thread.Sleep(10000);

            var task = Task.Run(async () =>
            {
                Console.WriteLine($"Starting auto peering...  {tasks.Count}");
                var c = 1;
                var rateLimit = 9000;
                var injectionCount = 75;
                foreach (var task in tasks)
                {
                    var h = Task.Factory.StartNew(() => task.Start(), TaskCreationOptions.LongRunning);
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

                    c++;
                }

                await Task.Delay(100 * total);
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");

                long v = 0;
                while (_running)
                {
                    foreach (var task in tasks)
                    {
                        //continue;
                        if (await task.Result.BootAsync(v).FastPath().ConfigureAwait(false))
                        {
                            v++;
                            break;
                        }
                    }

                    await Task.Delay(500).ConfigureAwait(false);
                }
                
            });

            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                Console.WriteLine("###");
            };

            Console.CancelKeyPress += (sender, args) =>
            {
                ZeroAsync(total);
                args.Cancel = true;
            };

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
                            opeers += ioCcNode.Neighbors.Values.Count(n => ((CcDrone)n).Adjunct?.IsDroneConnected ?? false);
                            var e = ioCcNode.EgressConnections;
                            var i = ioCcNode.IngressConnections;
                            minOut = Math.Min(minOut, e);
                            minIn = Math.Min(minIn, i);
                            if (ioCcNode.TotalConnections == 0)
                                empty++;
                            if (ioCcNode.EgressConnections == 0)
                                minOutC++;
                            if (ioCcNode.IngressConnections == 0)
                                minInC++;

                            if (ioCcNode.TotalConnections > ioCcNode.MaxDrones)
                            {
                                Console.WriteLine($"[{ioCcNode.Description}]");
                                foreach (var d in ioCcNode.Neighbors.Values)
                                {
                                    var drone = (CcDrone) d;
                                    Console.WriteLine($"[{ioCcNode.Description}] -> {drone.Description} ][ {drone.Adjunct.MetaDesc}, uptime = {drone.Uptime.ElapsedToSec():0.0}s");
                                }
                            }
                                

                            ooutBound += e;
                            oinBound += i;
                            oavailable += ioCcNode.Hub.Neighbors.Values.Count(static n => ((CcAdjunct)n).Proxy);
                            if ((uptimeCount = ioCcNode.Hub.Neighbors.Count - 1) > 0)
                                uptime += ioCcNode.Hub.Neighbors.Values.Select(static n =>
                                {
                                    if (((CcAdjunct)n).IsDroneConnected && ((CcAdjunct)n).AttachTimestamp > 0)
                                        return ((CcAdjunct)n).AttachTimestamp.Elapsed();
                                    
                                    return 0;
                                }).Sum();

                            uptimeCount += ioCcNode.TotalConnections;
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

                            try
                            {
                                Console.ForegroundColor = prevPeers <= peers ? ConsoleColor.Green : ConsoleColor.Red;
                                Console.WriteLine($"out = {outBound}, int = {inBound}, ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones} , peers = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / ((double)_nodes.Count * maxDrones) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt}), con = {CcAdjunct.ConnectionTime/(CcAdjunct.ConnectionCount + 1):0.0}ms");
                            }
                            catch{}
                            finally
                            {
                                Console.ResetColor();
                            }

                        }

                        if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - lastUpdate > 120)
                        {
                            ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                            ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                            ThreadPool.GetMinThreads(out var minwt, out var mincpt);

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"out = {outBound}, int = {inBound}, ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones} , peers = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / (double)(_nodes.Count * maxDrones) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, workers = {-wt + maxwt}, ports = {-cpt + maxcpt}, con = {CcAdjunct.ConnectionTime / (CcAdjunct.ConnectionCount + 1):0.0}ms");
                            Console.ResetColor();
                            lastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Thread.Sleep(30000);
                }
            }, TaskCreationOptions.LongRunning);

            Console.ReadLine();

            _running = false;
            
            
            _nodes.ToList().ForEachAsync<CcCollective,object>(static async (n,_) =>  await n.ZeroAsync(null).FastPath().ConfigureAwait(false)).GetAwaiter().GetResult();
            _nodes.Clear();


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

            var s = host.StopAsync();
            LogManager.Shutdown();
            Console.WriteLine("##");

            Console.ReadLine();
        }

        /// <summary>
        /// Tests queue
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private static async Task QueueTest() //TODO make unit tests
        {

            IoQueue<int> q = new IoQueue<int>("test", 2000000000, 10);
            var head = q.EnqueueAsync(1).FastPath().ConfigureAwait(false).GetAwaiter().GetResult();
            q.EnqueueAsync(2).FastPath().ConfigureAwait(false).GetAwaiter();
            q.EnqueueAsync(3).FastPath().ConfigureAwait(false).GetAwaiter();
            q.EnqueueAsync(4).FastPath().ConfigureAwait(false).GetAwaiter();
            var five = q.EnqueueAsync(5).FastPath().ConfigureAwait(false).GetAwaiter().GetResult();
            q.EnqueueAsync(6).FastPath().ConfigureAwait(false).GetAwaiter();
            q.EnqueueAsync(7).FastPath().ConfigureAwait(false).GetAwaiter();
            q.EnqueueAsync(8).FastPath().ConfigureAwait(false).GetAwaiter();
            var tail = q.EnqueueAsync(9).FastPath().ConfigureAwait(false).GetAwaiter().GetResult();

            Console.WriteLine("Init");
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }
            Console.WriteLine("\nDQ mid");
            q.RemoveAsync(five).FastPath().ConfigureAwait(false).GetAwaiter();
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            var c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Next;
            }
            
            Console.WriteLine("\nDQ head");
            q.RemoveAsync(q.Tail).FastPath().ConfigureAwait(false).GetAwaiter();
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Next;
            }
            
            Console.WriteLine("\nDQ tail");
            q.RemoveAsync(q.Head).FastPath().ConfigureAwait(false).GetAwaiter();
            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Next;
            }
            

            Console.WriteLine("\nDQ second last prime");
            q.DequeueAsync().FastPath().ConfigureAwait(false).GetAwaiter();
            q.RemoveAsync(q.Tail).FastPath().ConfigureAwait(false).GetAwaiter();
            q.RemoveAsync(q.Tail).FastPath().ConfigureAwait(false).GetAwaiter();
            q.DequeueAsync().FastPath().ConfigureAwait(false).GetAwaiter();

            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Next;
            }
            
            Console.WriteLine("\nDQ second last");

            if (true)
            {
                q.RemoveAsync(q.Tail).FastPath().ConfigureAwait(false).GetAwaiter();
                q.RemoveAsync(q.Tail).FastPath().ConfigureAwait(false).GetAwaiter();    
            }
            else
            {
                q.DequeueAsync().FastPath().ConfigureAwait(false).GetAwaiter();
                q.DequeueAsync().FastPath().ConfigureAwait(false).GetAwaiter();   
            }

            foreach (var ioZNode in q)
            {
                Console.Write(ioZNode.Value);
            }

            Console.WriteLine();
            c = q.Tail;
            while (c != null)
            {
                Console.Write(c.Value);
                c = c.Next;
            }

            var _concurrentTasks = new List<Task>();

            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var rounds = 10;
            var mult = 100000;
            for (var i = 0; i < rounds; i++)
            {
                var i3 = i;
                _concurrentTasks.Add(Task.Factory.StartNew(async () =>
                {
                    for (int j = 0; j < mult; j++)
                    {
                        try
                        {
                            await q.EnqueueAsync(i3).FastPath().ConfigureAwait(false);
                            await q.EnqueueAsync(i3 + 1).FastPath().ConfigureAwait(false);
                            //var i1 = q.EnqueueAsync(i3 + 2).FastPath().ConfigureAwait(false).GetAwaiter().GetResult();
                            //var i2 = q.EnqueueAsync(i3 + 3).FastPath().ConfigureAwait(false).GetAwaiter().GetResult();
                            await q.EnqueueAsync(i3 + 4).FastPath().ConfigureAwait(false);

                            //q.RemoveAsync(i2).FastPath().ConfigureAwait(false).GetAwaiter();
                            await q.DequeueAsync().FastPath().ConfigureAwait(false);
                            //q.RemoveAsync(i1).FastPath().ConfigureAwait(false).GetAwaiter();
                            await q.DequeueAsync().FastPath().ConfigureAwait(false);
                            await q.DequeueAsync().FastPath().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }
                    Console.Write($"({i3}-{q.Count})");
                }, new CancellationToken(), TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Current).Unwrap());
            }
            Task.WhenAll(_concurrentTasks).GetAwaiter().GetResult();
            
            Console.WriteLine($"count = {q.Count}, Head = {q?.Head?.Value}, tail = {q?.Tail?.Value}, time = {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start}ms, {rounds*mult*6/(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - start)} kOPS");
            
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
            var mutex = new IoZeroSemaphoreSlim(asyncTasks.Token, "zero slim", maxBlockers: capacity, maxAsyncWork:2, initialCount: 0, enableAutoScale: false, enableFairQ: false, enableDeadlockDetection: true);
            //var mutex = new IoZeroRefMut(asyncTasks.Token);

            var releaseCount = 1;
            var enableThrottle = true;
            var waiters = 3;
            var releasers = 3;
            var targetSleep = (long)0;
            var logSpam = 30000;//at least 1

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
            TaskCreationOptions options = TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach;

            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
            var t1 = Task.Factory.StartNew(async o =>
             {
                 try
                 {
                     while (waiters>0)
                     {
                         sw.Restart();
                         if (await mutex.WaitAsync().ConfigureAwait(false))
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
                             //     await Task.Delay(1).ConfigureAwait(false);
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
                        // await block.OverBoostAsync().ConfigureAwait(false);
                        // if(!block.Result)
                        //     break;
           
                        sw2.Restart();
                       if (await mutex.WaitAsync().ConfigureAwait(false))
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
                            //     await Task.Delay(1).ConfigureAwait(false);
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
                        // await block.OverBoostAsync().ConfigureAwait(false);
                        // if(!block.Result)
                        //     break;
           
                        sw2.Restart();
                       if (await mutex.WaitAsync().ConfigureAwait(false))
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
                               await Task.Delay((int)targetSleep, asyncTasks.Token).ConfigureAwait(false);

                           if (mutex.CurNrOfBlockers >= mutex.Capacity*4/5)
                           {
                               await Task.Delay(200).ConfigureAwait(false);
                           }
                           
                           try
                           {
                               Interlocked.Add(ref semCount, curCount = await mutex.ReleaseAsync(releaseCount, true).FastPath().ConfigureAwait(false));

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
                               await Task.Delay(10, asyncTasks.Token).ConfigureAwait(false);
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

        private static async ValueTask ZeroAsync(int total)
        {
            _running = false;
            await AutoPeeringEventService.ClearAsync().FastPath().ConfigureAwait(false);
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
                        var t = n.ZeroAsync(new IoNanoprobe("Zero.Sync"));
                        Interlocked.Increment(ref zeroed);
                        s.Release();
                    });

                    if (zeroed > 0 && zeroed % 100 == 0)
                    {
                        Console.WriteLine(
                            $"Estimated {TimeSpan.FromMilliseconds((_nodes.Count - zeroed) * (zeroed * 1000 / (sw.ElapsedMilliseconds + 1)))}, zeroed = {zeroed}/{_nodes.Count}");
                    }
                }
                catch
                {

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

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.ZeroAsync(new IoNanoprobe("process was exited")).ConfigureAwait(false);
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.ZeroAsync(new IoNanoprobe("ctrl + c, process was killed")).ConfigureAwait(false);
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
                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.ZeroAsync(new IoNanoprobe("process was exited")).ConfigureAwait(false);
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.ZeroAsync(new IoNanoprobe("ctrl + c, process was killed")).ConfigureAwait(false);
#pragma warning restore 4014
                tangleNodeTask.Wait();
            }
        }

        private static Task<CcCollective> CoCoonAsync(CcDesignation ccDesignation, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, List<string> bootStrapAddress, int total)
        {

            var cocoon = new CcCollective(ccDesignation,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                4, 2, 2, 1);

            _nodes.Add(cocoon);

#pragma warning disable 4014

            var t = new Task<CcCollective>(() =>
            {
                cocoon.EmitAsync().FastPath().ConfigureAwait(false);
                cocoon.StartAsync().ConfigureAwait(false);
                return cocoon;
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);
            return t;
        }
    }
}
