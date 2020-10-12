using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using NLog;
using zero.cocoon;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.semaphore;
using zero.tangle;
using zero.tangle.entangled;
using zero.tangle.models;

namespace zero.sync
{
    class Program
    {

        private static ConcurrentBag<IoCcNode> _nodes = new ConcurrentBag<IoCcNode>();
        private static volatile bool _running;

        static void Main(string[] args)
        {
            //Test();
            LogManager.LoadConfiguration("nlog.config");
            var portOffset = 3000;

#if DEBUG
            portOffset = 0;
#endif

            var random = new Random((int)DateTime.Now.Ticks);
            //Tangle("tcp://192.168.1.2:15600");
            int total = 2000;
            var maxNeighbors = 8;
            var tasks = new ConcurrentBag<Task<IoCcNode>>();
            tasks.Add(CoCoonAsync(IoCcIdentity.Generate(true), $"tcp://127.0.0.1:{14667 + portOffset}", $"udp://127.0.0.1:{14627 + portOffset}", $"tcp://127.0.0.1:{11667 + portOffset}", $"udp://127.0.0.1:{14627 + portOffset}", new[] { $"udp://127.0.0.1:{14626 + portOffset}" }.ToList(), 0));

            tasks.Add(CoCoonAsync(IoCcIdentity.Generate(), $"tcp://127.0.0.1:{15670 + portOffset}", $"udp://127.0.0.1:{15630 + portOffset}", $"tcp://127.0.0.1:{11667 + portOffset}", $"udp://127.0.0.1:{15630 + portOffset}", new[] { $"udp://127.0.0.1:{14627 + portOffset}", $"udp://127.0.0.1:{15631 + portOffset}" }.ToList(), 1));

            for (var i = 2; i < total; i++)
            {
                //tasks.Add(CoCoonAsync(IoCcIdentity.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", Enumerable.Range(0, 16).Select(i => $"udp://127.0.0.1:{15629 + portOffset + random.Next(total - 1)/* % (total/6 + 1)*/}").ToList(), i));
                tasks.Add(CoCoonAsync(IoCcIdentity.Generate(), $"tcp://127.0.0.1:{15669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", $"tcp://127.0.0.1:{11669 + portOffset + i}", $"udp://127.0.0.1:{15629 + portOffset + i}", new[] { $"udp://127.0.0.1:{15630 + portOffset + i - 2}", $"udp://127.0.0.1:{15630 + portOffset + (total - i + 2) % (total - 2)}", $"udp://127.0.0.1:{15630 + portOffset + Math.Abs(total/2 - i + 2)%(total - 2)}", $"udp://127.0.0.1:{15630 + portOffset + (total / 2 + i - 2) % (total - 2)}", $"udp://127.0.0.1:{15629 + portOffset + random.Next(total - 2)}" }.ToList(), i));
                if (tasks.Count % 10 == 0)
                    Console.WriteLine($"Spawned {tasks.Count}/{total}...");
            }

            var task = Task.Run(async () =>
            {
                Console.WriteLine($"Starting auto peering...  {tasks.Count}");
                var c = 0;
                var rateLimit = 11000;
                foreach (var task in tasks)
                {
                    task.Start();
                    c++;
                    if (c % 150 == 0)
                    {
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");
                        Console.WriteLine($"Provisioned {c}/{total}...");

                        await Task.Delay(rateLimit+=20 * 150).ConfigureAwait(false);
                    }
                }

                await Task.Delay(60 * total);
                Console.WriteLine($"Starting accounting... {tasks.Count}");
                c = 0;
                foreach (var task in tasks)
                {
                    //continue;
                    await task.Result.BootAsync();
                    c++;
                    //if (c % 20 == 0)
                    {
                        Console.WriteLine($"<<<Testing {c}/{total}...>>>");
                        //Thread.Sleep(1000);
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
                long lastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                while (_running)
                {
                    try
                    {
                        ooutBound = 0;
                        oinBound = 0;
                        oavailable = 0;
                        opeers = 0;

                        uptime = 0;
                        uptimeCount = 1;
                        foreach (var ioCcNode in _nodes)
                        {
                            opeers += ioCcNode.Neighbors.Values.Count(n => ((IoCcPeer)n).Neighbor?.IsPeerConnected??false);
                            ooutBound += ioCcNode.EgressConnections;
                            oinBound += ioCcNode.IngressConnections;
                            oavailable += ioCcNode.DiscoveryService.Neighbors.Values.Count(n => ((IoCcNeighbor)n).Proxy);
                            if (ioCcNode.DiscoveryService.Neighbors.Count > 0)
                                uptime += (long)(ioCcNode.DiscoveryService.Neighbors.Values.Select(n =>
                                {
                                    if (((IoCcNeighbor)n).IsPeerConnected && ((IoCcNeighbor)n).AttachTimestamp > 0)
                                    {
                                        uptimeCount++;
                                        return ((IoCcNeighbor)n).AttachTimestamp.Elapsed();
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
                            Console.WriteLine($"out = {outBound}, int = {inBound}, {inBound + outBound}/{_nodes.Count * maxNeighbors} , peers = {peers}/{available}, {(inBound + outBound) / ((double)_nodes.Count * maxNeighbors) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, ({minwt} < {wt} < {maxwt}), ({mincpt} < {cpt} < {maxcpt})");
                            Console.ResetColor();
                        }

                        if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - lastUpdate > 120)
                        {
                            ThreadPool.GetAvailableThreads(out var wt, out var cpt);
                            ThreadPool.GetMaxThreads(out var maxwt, out var maxcpt);
                            ThreadPool.GetMinThreads(out var minwt, out var mincpt);

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"out = {outBound}, int = {inBound}, {inBound + outBound}/{(double)_nodes.Count * maxNeighbors} , peers = {peers}/{available}, {(inBound + outBound) / (double)(_nodes.Count * maxNeighbors) * 100:0.00}%, uptime = {TimeSpan.FromSeconds(uptime / uptimeCount)}, total = {TimeSpan.FromSeconds(uptime).TotalDays:0.00} days, workers = {-wt + maxwt}, ports = {-cpt + maxcpt}");
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

        class MutexClass : IIoMutex
        {
            public IIoMutex AsyncMutex;
            public void Configure(CancellationTokenSource asyncTasks, bool signalled = false, bool allowInliningContinuations = true)
            {
                 AsyncMutex = new IoAsyncMutex(asyncTasks);
                 AsyncMutex.ByRef(ref AsyncMutex);
                
                 AsyncMutex.Configure(asyncTasks, signalled, allowInliningContinuations);
            }

            public void Set()
            {
                Console.WriteLine($"WAITED IS {AsyncMutex.GetWaited()}");
                Console.WriteLine($"HOOKED IS {AsyncMutex.GetHooked()}");
                AsyncMutex.Set();
            }

            public ValueTask<bool> WaitAsync()
            {
                var retval = AsyncMutex.WaitAsync();
                Console.WriteLine($"WAITED SHOULD BE one => {AsyncMutex.GetWaited()}");
                return retval;
            }

            public int GetWaited()
            {
                throw new NotImplementedException();
            }

            public void SetWaited()
            {
                throw new NotImplementedException();
            }

            public int GetHooked()
            {
                throw new NotImplementedException();
            }

            public void SetHooked()
            {
                throw new NotImplementedException();
            }

            public void SetResult(bool result)
            {
                throw new NotImplementedException();
            }

            public void Zero()
            {
                throw new NotImplementedException();
            }

            public void ByRef(ref IIoMutex root)
            {
                throw new NotImplementedException();
            }

            public short Version()
            {
                throw new NotImplementedException();
            }

            public ref IIoMutex GetRef(ref IIoMutex mutex)
            {
                throw new NotImplementedException();
            }

            public short GetCurFrame()
            {
                throw new NotImplementedException();
            }

            public bool SetWaiter(Action<object> continuation, object state)
            {
                throw new NotImplementedException();
            }

            public bool GetResult(short token)
            {
                throw new NotImplementedException();
            }

            public ValueTaskSourceStatus GetStatus(short token)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            {
                throw new NotImplementedException();
            }
        }
        
        private static void Test()
        {
            CancellationTokenSource asyncTasks = new CancellationTokenSource();
            //IIoSemaphore sem = new IoSemaphoreOne<IoAutoMutex>(asyncTasks, 1);
            
            // var mutex = new MutexClass
            // {
            //     AsyncMutex = new IoAsyncMutex( asyncTasks)
            // };
            
             // var mutex = new MutexClass();
             // mutex.Configure(asyncTasks);
             var capacity = 50000;
            var mutex = new IoZeroSemaphoreSlim(asyncTasks, "zero slim", capacity, 1, false, false, true);
            //var mutex = new IoZeroNativeMutex(asyncTasks);

            var releaseCount = 2;
            var enableThrottle = true;
            var twoWaiters = true;
            var twoReleasers = 2;
            var targetSleep = (long) 0;
            var targetSleepMult = twoWaiters ? 2 : 1;
            var logSpam = 3000;
            var sw = new Stopwatch();    
            var sw2 = new Stopwatch();
            var c = 0;
            long semCount = 0;
            long semPollCount = 0;
            IoFpsCounter wfps1 = new IoFpsCounter(1000,10000);
            IoFpsCounter wfps2 = new IoFpsCounter(1000,10000);
            IoFpsCounter ifps1 = new IoFpsCounter(1000,10000);
            IoFpsCounter ifps2 = new IoFpsCounter(1000,10000);
            //TaskCreationOptions options = TaskCreationOptions.LongRunning | TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously;
            TaskCreationOptions options = TaskCreationOptions.None;
            
            
            
            var t1= Task.Factory.StartNew(async o =>
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
                                >5 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                <-5 => () => Console.ForegroundColor = ConsoleColor.DarkRed,
                                _ => () => Console.ForegroundColor = ConsoleColor.DarkGreen,
                            };
                            a();
                            //Console.WriteLine($"T1:{mut.AsyncMutex}({++c}) t = {tt - targetSleep}ms, {fps.Fps(): 00.0}");
                            if (Interlocked.Increment(ref c) % logSpam == 0)
                                Console.WriteLine($"T1:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount/(double)semPollCount:0.0}, S = {mutex.CurrentCount}");
                            Console.ResetColor();
                        }
                        else
                        {
                            break;
                            var tt = sw.ElapsedMilliseconds;
                            wfps1.Tick();
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"F1:{mutex}({--c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount/(double)semPollCount:0.0},  S = {mutex.CurrentCount}");
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

            var t2  = Task.Factory.StartNew(async o =>
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
                                Console.WriteLine($"T2:{mutex}({c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount/(double)semPollCount:0.0}, S = {mutex.CurrentCount}");
                            Console.ResetColor();
                        }
                        else
                        {
                            break;
                            var tt = sw.ElapsedMilliseconds;
                            wfps2.Tick();
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"F2:{mutex}({--c}) t = {tt - targetSleep * targetSleepMult}ms, [{wfps1.Fps() + wfps2.Fps(): 0}, ({wfps1.Fps(): 0}, {wfps2.Fps(): 0})], [{ifps1.Fps() + ifps2.Fps(): 0} ({ifps1.Fps(): 0}, {ifps2.Fps(): 0})], s = {semCount/(double)semPollCount:0.0}, S = {mutex.CurrentCount}");
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

            var t3 = Task.Factory.StartNew(async o=>
            {

                try
                {
                    var curCount = 1;
                    while (twoReleasers > 0)
                    {
                        if (targetSleep > 0)
                            await Task.Delay((int) targetSleep).ConfigureAwait(false);
                        try
                        {
                            Interlocked.Add(ref semCount, curCount = mutex.Release(releaseCount));
                        }
                        catch (SemaphoreFullException)
                        {
                            var f = wfps1.Fps() + wfps2.Fps() + 1;
                            
                            var d = mutex.CurrentCount / (f) * 1000.0;
                            var val = (int) d;
                            
                            Console.WriteLine($"Throttling: {val} ms, curCount = {mutex.CurrentCount}");
                            await Task.Delay(Math.Max(1,val), asyncTasks.Token).ConfigureAwait(false);
                        }
                        catch (TaskCanceledException )
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            
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

            var t4 = Task.Factory.StartNew(async o=>
            {

                try
                {
                    var curCount = 1;
                    
                    while (twoReleasers > 1)
                    {
                        if (targetSleep > 0)
                            await Task.Delay((int) targetSleep).ConfigureAwait(false);

                        try
                        {
                            Interlocked.Add(ref semCount, curCount = mutex.Release(releaseCount));
                        }
                        catch (SemaphoreFullException)
                        {
                            var f = wfps1.Fps() + wfps2.Fps() + 1;

                            var d = mutex.CurrentCount / (f) * 1000.0;
                            var val = (int) d;

                            Console.WriteLine($"Throttling: {val} ms, curCount = {mutex.CurrentCount}");
                            await Task.Delay(Math.Max(1, val), asyncTasks.Token).ConfigureAwait(false);
                        }
                        catch (TaskCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            
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

                if (zeroed > 0 && zeroed % 100 == 0)
                {
                    Console.WriteLine(
                        $"Estimated {TimeSpan.FromMilliseconds((_nodes.Count - zeroed) * (zeroed * 1000 / (sw.ElapsedMilliseconds + 1)))}, zeroed = {zeroed}/{_nodes.Count}");
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
                0, 0, 8,1);

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
