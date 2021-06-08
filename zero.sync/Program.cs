using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
                        .ConfigureLogging(builder =>
                        {
                            builder.ClearProviders();
                            builder.SetMinimumLevel(LogLevel.None);
                        })
                        .UseNLog();
                });

        static void Main(string[] args)
        {
            //Test();
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
            var total = 30;
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
                tasks.Add(CoCoonAsync(CcDesignation.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", new[] { $"udp://127.0.0.1:{15629 + portOffset + i - 1}", $"udp://127.0.0.1:{15629 + portOffset + i - 3}" }.ToList(), i));
                if (tasks.Count % 10 == 0)
                    Console.WriteLine($"Spawned {tasks.Count}/{total}...");
            }

            //Console.WriteLine("Prepping...");
            //Thread.Sleep(10000);

            var task = Task.Run(async () =>
            {
                Console.WriteLine($"Starting auto peering...  {tasks.Count}");
                var c = 1;
                var rateLimit = 5000;
                foreach (var task in tasks)
                {
                    var h = Task.Factory.StartNew(() => task.Start(), TaskCreationOptions.LongRunning);
                    if (c % 50 == 0)
                    {
                        await Task.Delay(rateLimit += 200).ConfigureAwait(false);

                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                    }

                    c++;
                }

                await Task.Delay(200 * total);
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                Console.WriteLine($"Starting accounting... {tasks.Count}");

                c = 0;
                long v = 0;
                while (_running)
                {
                    foreach (var task in tasks)
                    {
                        //continue;
                        if (await task.Result.BootAsync(v))
                        {
                            v++;
                            break;
                        }

                        c++;
                        //if (c % 20 == 0)
                        {
                            //Console.WriteLine($"<<<Testing {c}/{total}...>>>");
                            //Thread.Sleep(1000);
                        }
                    }

                    await Task.Delay(100).ConfigureAwait(false);
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
                                    Console.WriteLine($"[{ioCcNode.Description}] -> {drone.Description} ][ {drone.Adjunct.MetaDesc}, uptime = {drone.Uptime.TickSec():0.00}");
                                }
                            }
                                

                            ooutBound += e;
                            oinBound += i;
                            oavailable += ioCcNode.Hub.Neighbors.Values.Count(n => ((CcAdjunct)n).Proxy);
                            if (ioCcNode.Hub.Neighbors.Count > 0)
                                uptime += (long)(ioCcNode.Hub.Neighbors.Values.Select(n =>
                                {
                                    if (((CcAdjunct)n).IsDroneConnected && ((CcAdjunct)n).AttachTimestamp > 0)
                                    {
                                        uptimeCount++;
                                        return ((CcAdjunct)n).AttachTimestamp.Elapsed();
                                    }
                                    return 0;
                                }).Sum());
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

                            Console.ForegroundColor = prevPeers <= peers ? ConsoleColor.Green : ConsoleColor.Red;
                            Console.WriteLine($"out = {outBound}, int = {inBound}, ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones} , peers = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / ((double)_nodes.Count * maxDrones) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt}), con = {CcAdjunct.ConnectionTime/CcAdjunct.ConnectionCount:0.0}ms");
                            Console.ResetColor();
                        }

                        if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - lastUpdate > 120)
                        {
                            ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                            ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                            ThreadPool.GetMinThreads(out var minwt, out var mincpt);

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"out = {outBound}, int = {inBound}, ({minOut}/{minOutC},{minIn}/{minInC}::{empty}), {inBound + outBound}/{(double)_nodes.Count * maxDrones} , peers = {peers}/{available}({_nodes.Count * maxAdjuncts}), {(inBound + outBound) / (double)(_nodes.Count * maxDrones) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, workers = {-wt + maxwt}, ports = {-cpt + maxcpt}, con = {CcAdjunct.ConnectionTime / CcAdjunct.ConnectionCount:0.0}ms");
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

            var s = host.StopAsync();
            LogManager.Shutdown();
            Console.WriteLine("##");

            Console.ReadLine();
        }

        private static void Test()
        {
            CancellationTokenSource asyncTasks = new CancellationTokenSource();

            var capacity = 1000;
            var mutex = new IoZeroSemaphoreSlim(asyncTasks, "zero slim", capacity, 1, false, false, true);
            //var mutex = new IoZeroNativeMutex(asyncTasks);

            var releaseCount = 1;
            var enableThrottle = true;
            var twoWaiters = true;
            var twoReleasers = 2;
            var targetSleep = (long)0;
            var logSpam = 30000;
            var targetSleepMult = twoWaiters ? 2 : 1;
            var sw = new Stopwatch();
            var sw2 = new Stopwatch();
            var c = 0;
            long semCount = 0;
            long semPollCount = 0;
            IoFpsCounter wfps1 = new IoFpsCounter(1000, 10000);
            IoFpsCounter wfps2 = new IoFpsCounter(1000, 10000);
            IoFpsCounter ifps1 = new IoFpsCounter(1000, 10000);
            IoFpsCounter ifps2 = new IoFpsCounter(1000, 10000);
            //TaskCreationOptions options = TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously;
            TaskCreationOptions options = TaskCreationOptions.None;


            var t1 = Task.Factory.StartNew(async o =>
             {
                 try
                 {
                     while (true)
                     {
                        // var block = sem.WaitAsync();
                        // await block.OverBoostAsync().ConfigureAwait(false);
                        // if(!block.Result)
                        //     break;

                        sw.Restart();
                         if (await mutex.WaitAsync().ConfigureAwait(false))
                         {
                             var tt = sw.ElapsedMilliseconds;
                             wfps1.Tick();

                             Action a = (tt - targetSleep * targetSleepMult) switch
                             {
                                 > 5 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                 < -5 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                 _ => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                             };
                             a();
                            //Console.WriteLine($"T1:{mut.AsyncMutex}({++c}) t = {tt - targetSleep}ms, {fps.Fps(): 00.0}");
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                                 Console.WriteLine($"T1:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount / (double)semPollCount:0.0}, S = {mutex.CurrentCount}");
                             Console.ResetColor();
                         }
                         else
                         {
                             break;
                             var tt = sw.ElapsedMilliseconds;
                             wfps1.Tick();
                             Console.ForegroundColor = ConsoleColor.Yellow;
                             Console.WriteLine($"F1:{mutex}({--c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount / (double)semPollCount:0.0},  S = {mutex.CurrentCount}");
                             Console.ResetColor();
                             await Task.Delay(500).ConfigureAwait(false);
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
                   while (twoWaiters)
                   {
                        // var block = sem.WaitAsync();
                        // await block.OverBoostAsync().ConfigureAwait(false);
                        // if(!block.Result)
                        //     break;

                        sw2.Restart();
                       if (await mutex.WaitAsync().ConfigureAwait(false))
                       {
                           var tt = sw2.ElapsedMilliseconds;
                           wfps2.Tick();

                           Action a = (tt - targetSleep * targetSleepMult) switch
                           {
                               > 5 => () => Console.ForegroundColor = ConsoleColor.Red,
                               < -5 => () => Console.ForegroundColor = ConsoleColor.Red,
                               _ => () => Console.ForegroundColor = ConsoleColor.Green,
                           };
                           a();
                            //Console.WriteLine($"T2:{mut.AsyncMutex}({++c}) t = {tt - targetSleep}ms, {fps.Fps(): 00.0}");
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                               Console.WriteLine($"T2:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount / (double)semPollCount:0.0}, S = {mutex.CurrentCount}");
                           Console.ResetColor();
                       }
                       else
                       {
                           break;
                           var tt = sw.ElapsedMilliseconds;
                           wfps2.Tick();
                           Console.ForegroundColor = ConsoleColor.Yellow;
                           Console.WriteLine($"F2:{mutex}({--c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount / (double)semPollCount:0.0}, S = {mutex.CurrentCount}");
                           Console.ResetColor();
                           await Task.Delay(500).ConfigureAwait(false);
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
                    var curCount = 1;
                    while (twoReleasers > 0)
                    {
                        if (targetSleep > 0)
                            await Task.Delay((int)targetSleep).ConfigureAwait(false);
                        try
                        {
                            Interlocked.Add(ref semCount, curCount = mutex.Release(releaseCount));
                        }
                        catch (SemaphoreFullException)
                        {
                            var f = wfps1.Fps() + wfps2.Fps() + 1;

                            var d = mutex.CurrentCount / (f) * 1000.0;
                            var val = (int)d;

                            Console.WriteLine($"Throttling: {val} ms, curCount = {mutex.CurrentCount}");
                            await Task.Delay(Math.Max(1, val), asyncTasks.Token).ConfigureAwait(false);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Interlocked.Increment(ref semPollCount);
                        ifps1.Tick();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"3:{e}");
                    throw;
                }
            }, asyncTasks.Token, options);

            var t4 = Task.Factory.StartNew(async o =>
            {

                try
                {
                    var curCount = 1;

                    while (twoReleasers > 1)
                    {
                        if (targetSleep > 0)
                            await Task.Delay((int)targetSleep).ConfigureAwait(false);

                        try
                        {
                            Interlocked.Add(ref semCount, curCount = mutex.Release(releaseCount));
                        }
                        catch (SemaphoreFullException)
                        {
                            var f = wfps1.Fps() + wfps2.Fps() + 1;

                            var d = mutex.CurrentCount / (f) * 1000.0;
                            var val = (int)d;

                            Console.WriteLine($"Throttling: {val} ms, curCount = {mutex.CurrentCount}");
                            await Task.Delay(Math.Max(1, val), asyncTasks.Token).ConfigureAwait(false);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch
                        {
                            // ignored
                        }


                        Interlocked.Increment(ref semPollCount);
                        ifps2.Tick();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"3:{e}");
                    throw;
                }
            }, asyncTasks.Token, options);



            Console.ReadLine();
            Console.WriteLine("TEARDOWN");
            asyncTasks.Cancel();
            Console.ReadLine();
            Console.WriteLine("Done");
            Console.ReadLine();
        }

        private static void Zero(int total)
        {
            _running = false;
            AutoPeeringEventService.Clear();
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
                        n.ZeroAsync(new IoNanoprobe("Zero.Sync")).ConfigureAwait(false);
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

        private static Task<CcCollective> CoCoonAsync(CcDesignation ccDesignation, string gossipAddress, string peerAddress,
            string fpcAddress, string extAddress, List<string> bootStrapAddress, int total)
        {

            var cocoon = new CcCollective(ccDesignation,
                IoNodeAddress.Create(gossipAddress),
                IoNodeAddress.Create(peerAddress),
                IoNodeAddress.Create(fpcAddress),
                IoNodeAddress.Create(extAddress),
                bootStrapAddress.Select(IoNodeAddress.Create).Where(a => a.Port.ToString() != peerAddress.Split(":")[2]).ToList(),
                0, 0, 16, 1);

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

            return new Task<CcCollective>(() =>
            {
                cocoon.StartAsync().ConfigureAwait(false);
                return cocoon;
            }, TaskCreationOptions.None);
        }
    }
}
