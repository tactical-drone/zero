using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using NLog;
using zero.core.misc;
using zero.core.patterns.misc;
using zero.core.patterns.queue;


namespace zero.cocoon.events.services
{
    public class AutoPeeringEventService: autopeering.autopeeringBase
    {
        public AutoPeeringEventService(ILogger<AutoPeeringEventService> logger)
        {
            _logger = logger;
        }

        public static int Port { get; set; }

        private const int EventBatchSize = 4096;
        private const int TotalBatches = 10;
        private readonly ILogger<AutoPeeringEventService> _logger;
        public static IoZeroQ<AutoPeerEvent>[] QueuedEvents =
        {
            //TODO tuning
            new($"{nameof(AutoPeeringEventService)}", EventBatchSize<<6, true, concurrencyLevel: 1),
        };

        private static volatile int _operational = 0;
        private static long _seq;
        private static volatile int _curIdx = 0;
        public static bool Operational => _operational > 0;


        private static readonly ConcurrentDictionary<int, Process> Processes = new();

        public static async Task ToggleActiveAsync()
        {
            _operational = _operational > 0 ? 0 : 1;
            if (!Operational)
            {
                await QueuedEvents[_curIdx % 2].ZeroManagedAsync<object>().FastPath();
                //await QueuedEvents[(_curIdx + 1) % 2].ZeroManagedAsync<object>().FastPath();
            }
        }

        public override async Task<EventResponse> Next(NullMsg request, ServerCallContext context)
        {
            
            var response = new EventResponse();

            if (_operational == 0)
                return response;

            try
            {
                //IoQueue<AutoPeerEvent> curQ = _queuedEvents[(Interlocked.Increment(ref _curIdx) - 1) % 2];
                IoZeroQ<AutoPeerEvent> curQ = QueuedEvents[0];

                int c = 0;
                while (curQ.Count == 0 && c++ < 20)
                    await Task.Delay(1000);

                c = 0;
                AutoPeerEvent cur;
                while (c++ < EventBatchSize && curQ.TryDequeue(out cur))
                {
                    response.Events.Add(cur);
                }

                //var cur = curQ.Head;
                //int c = 0;
                ////while (c < EventBatchSize && cur != null)
                //while (cur != null)
                //{
                //    response.Events.Add(cur.Value);
                //    cur = cur.Next;
                //    c++;
                //}

                //await curQ.ClipAsync(cur);

                //Console.WriteLine($"c = {c}");
                //await curQ.ClearAsync();
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }
            finally
            {
                //if(curQ.Count == 0)
                //    Interlocked.Increment(ref _curIdx);
            }

            return response;
        }

        static void SendResponse(string value, ShellCommand request, ShellCommand response = null)
        {

            response ??= new ShellCommand
            {
                Seq = request.Seq,
                Id = request.Id,
                Command = request.Command,
                Response = value
            };


#if DEBUG
            LogManager.GetCurrentClassLogger().Fatal($"--> \n{response.Response}\n");
#endif

            AddEvent(new AutoPeerEvent
            {
                EventType = AutoPeerEventType.ShellMsg,
                Shell = response
            });
        }

        public static void TaskKill(int pid)
        {
            var procStartInfo = new ProcessStartInfo("taskkill", $"/T /F /PID {pid}")
            {
                RedirectStandardOutput = false,
                RedirectStandardError = false,
                RedirectStandardInput = false,
                CreateNoWindow = true
            };

            var proc = new Process();
            proc.StartInfo = procStartInfo;
            proc.Start();
        }

        public override async Task<Response> Shell(ShellCommand request, ServerCallContext context)
        {
            var response = new Response();
            if (!string.IsNullOrEmpty(request.Command))
            {
                
                await Task.Factory.StartNew(static async state =>
                {
                    var (@this,request) = (ValueTuple<AutoPeeringEventService,ShellCommand>)state;
                    var isZero = request.Command.Contains("zero.sync");
                    //windows
                    if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                    {
                        //signals
                        if (request.Done)
                        {
                            switch (request.Result)
                            {
                                case "ctrl+c":
                                    if (Processes.TryGetValue(request.Uid, out var process))
                                    {
                                        LogManager.GetCurrentClassLogger().Warn($"ctrl+c: process {process.Id}, cmd = `{process.StartInfo.FileName} {process.StartInfo.Arguments}'");

                                        if(!process.HasExited)
                                            TaskKill(process.Id);
                                    }
                                    break;
                            }

                            return;
                        }

                        var procStartInfo = new ProcessStartInfo("cmd", "/c " + request.Command)
                        {
                            RedirectStandardOutput = true,
                            RedirectStandardError = true,
                            RedirectStandardInput = true,
                            CreateNoWindow = true
                        };

                        var proc = new Process();
                        proc.StartInfo = procStartInfo;
                        proc.Start();

                        if (!Processes.TryAdd(request.Uid, proc))
                        {
                            proc.Kill();
                            proc.Close();
                            proc.Dispose();
                            throw new InvalidOperationException($"FATAL: Unable to add process id {proc.Id}; [{request.Seq}], id = {request.Id}, cmd = {request.Command}, killing.");
                        }
                        else
                        {
                            LogManager.GetCurrentClassLogger().Info($"shell: command pid {proc.Id} executed; [{request.Seq}] - {request.Id},  cmd = `{request.Command}', current = {Processes.Count}");
                        }


                        var ts = Environment.TickCount;
                        try
                        {
                            const int bufSize = 384;
                            char [] buffer = new char[bufSize];
                            StringBuilder sb = new StringBuilder();
                            int read = -1;
                            var lines = 0;
                            bool hasData;
                            int peakCycle = 0;

                            //LogManager.GetCurrentClassLogger().Info($"Polling");
                            await proc.StandardOutput.BaseStream.FlushAsync();
                            while (!(proc.HasExited && proc.StandardOutput.EndOfStream) && 
                                   ( ((hasData = peakCycle++ % 2 == 0 ) || (hasData = proc.StandardOutput.Peek() != -1)) && 
                                       (read = await proc.StandardOutput.ReadBlockAsync(buffer, 0, bufSize)) > 0 || !hasData))
                            {
                                //LogManager.GetCurrentClassLogger().Info($"read = {read}, ready = {hasData}, time = {ts.ElapsedMs()} ms");

                                var flush = false;
                                //retry om empty reads, else dump the buffer
                                if (!hasData || read <= 0)
                                {
                                    if (sb.Length == 0)
                                    {
                                        await Task.Delay(100);
                                        continue;
                                    }
                                    flush = proc.StandardOutput.Peek() == -1;
                                }

                                if (read > 0)
                                {
                                    var output = new string(buffer, 0, read);
                                    read = -1;

                                    for (var l = 0; l < output.Length; l++)
                                    {
                                        if (output[l] == '\n')
                                            lines++;
                                    }

                                    if (isZero)
                                    {
                                        output = output.Replace(" INFO [", " <color=#ffffffff><b>INFO</b></color> [");
                                        output = output.Replace(" DEBUG [", " <color=#00ffffff><b>DEBUG</b></color> [");
                                        output = output.Replace(" ERROR [", " <color=#ffff00ff><b>ERROR</b></color> [");
                                        output = output.Replace(" WARN [", " <color=#ff00ffff><b>WARN</b></color> [");
                                        output = output.Replace(" FATAL [", " <color=#ff0000ff><b>FATAL</b></color> [");
                                        output = output.Replace(" TRACE [", " <color=#ffa500ff><b>TRACE</b></color> [");
                                        Interlocked.MemoryBarrier();
                                    }

                                    sb.Append(output);
                                }

                                if (ts.ElapsedMs() > 250 || lines > RandomNumberGenerator.GetInt32(2,24))
                                {
                                    int i;
                                    for (i = sb.Length - 1; i-- > 0; )
                                    {
                                        if(sb[i] == '\n')
                                            break;
                                    }

                                    if (i > 0)
                                    {
                                        request.Time = ts.ElapsedMs();
                                        var s = sb.ToString();
                                        var r = s[..(i + 1)];

                                        SendResponse(r, request);

                                        lines = 0;
                                        sb.Clear();

                                        if(i + 1 < s.Length)
                                            sb.Append(s[(i + 1)..]);
                                    }

                                    ts = Environment.TickCount;
                                }
                                else if (flush)
                                {
                                    SendResponse(sb.ToString(), request);
                                    sb.Clear();
                                    lines = 0;
                                }

                                await proc.StandardOutput.BaseStream.FlushAsync();
                            }

                            request.Time = ts.ElapsedMs();
                            SendResponse(sb.ToString(), request);
                            sb.Clear();
                        }
#if DEBUG
                        catch (Exception e)
                        {
                            var response = new ShellCommand
                            {
                                Seq = request.Seq,
                                Uid = request.Uid,
                                Id = request.Id,
                                Command = request.Command,
                                Response = $"<color=red>{e.Message}</color>",
                                Result = proc.ExitCode.ToString(), 
                                Time = ts.ElapsedMs(),
                                Done = true
                            };

                            SendResponse(null, null, response);
                        }
#endif
                        finally
                        {
                            //read any error data
                            var errorStr = await proc.StandardError.ReadToEndAsync();
                            if (!string.IsNullOrEmpty(errorStr))
                                errorStr = $"<color=red>{errorStr}</color>";

                            var response = new ShellCommand
                            {
                                Seq = request.Seq,
                                Uid = request.Uid,
                                Id = request.Id,
                                Command = request.Command,
                                Response = errorStr,
                                Result = proc.ExitCode.ToString(),
                                Time = ts.ElapsedMs(),
                                Done = true
                            };

                            SendResponse(null, null, response);

                            LogManager.GetCurrentClassLogger().Error(Processes.TryRemove(request.Uid, out _)
                                ? $"shell: [SUCCESS]; [{response.Seq}] - {request.Id}, cmd = {request.Command}"
                                : $"error; unable to remove process; pid = {proc.Id}, [{response.Seq}] - {request.Id}, cmd = {request.Command}");
                        }
                    }
                   

                }, (this,request), CancellationToken.None,TaskCreationOptions.HideScheduler | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

                response.Status = 0;
            }
            else
            {
                response.Status = 1;
                response.Message = $"error: invalid command parameters; seq = {request.Seq}, id = {request.Id}";
            }
            return response;
        }
        public static void AddEvent(AutoPeerEvent newAutoPeerEvent)
        {
            try
            {
                var curQ = QueuedEvents[_curIdx % 2];
                if (_operational > 0 && curQ.Count < curQ.Capacity)
                {
                    newAutoPeerEvent.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    newAutoPeerEvent.Seq = Interlocked.Increment(ref _seq);
                    curQ.TryEnqueue(newAutoPeerEvent);
                }
                else
                {
                    LogManager.GetCurrentClassLogger().Fatal($"Shutting down event stream! {curQ.Description}");
                    LogManager.GetCurrentClassLogger().Fatal($"Shutting down event stream! {curQ.Description}");
                    LogManager.GetCurrentClassLogger().Fatal($"Shutting down event stream! {curQ.Description}");
                    _operational = 0;
                    _ = Task.Run(ZeroAsync);
                }
            }
            catch
            {
                // ignored
            }

            //if(newAutoPeerEvent.EventType == AutoPeerEventType.RemoveDrone)
            //    LogManager.GetCurrentClassLogger().Error($"[{newAutoPeerEvent.Seq}] => {newAutoPeerEvent.EventType}: <<{newAutoPeerEvent.Drone.CollectiveId}| {newAutoPeerEvent.Drone.Id} >");

            //if (newAutoPeerEvent.EventType == AutoPeerEventType.RemoveAdjunct)
            //    LogManager.GetCurrentClassLogger().Error($"[{newAutoPeerEvent.Seq}] => {newAutoPeerEvent.EventType}: <<{newAutoPeerEvent.Adjunct.CollectiveId}| {newAutoPeerEvent.Adjunct.Id} >");

            //if (newAutoPeerEvent.EventType == AutoPeerEventType.AddDrone)
            //    LogManager.GetCurrentClassLogger().Error($"[{newAutoPeerEvent.Seq}] => {newAutoPeerEvent.EventType}: <<{newAutoPeerEvent.Drone.CollectiveId}| {newAutoPeerEvent.Drone.Id} >");

            if (newAutoPeerEvent.EventType == AutoPeerEventType.AddAdjunct)
                LogManager.GetCurrentClassLogger().Error($"[{newAutoPeerEvent.Seq}] => {newAutoPeerEvent.EventType}: <<{newAutoPeerEvent.Adjunct.CollectiveId}| {newAutoPeerEvent.Adjunct.Id} >");
        }


        /// <summary>
        /// Clears all buffers
        /// </summary>
        public static async ValueTask ClearAsync()
        {
            var q = QueuedEvents;
            if (QueuedEvents == null)
                return;

            await q[0].ZeroManagedAsync<object>(zero: false).FastPath();
            //await q[1].ZeroManagedAsync<object>(zero:true).FastPath();
        }

        /// <summary>
        /// Clears all buffers
        /// </summary>
        public static async ValueTask ZeroAsync()
        {
            Interlocked.Exchange(ref _operational, 0);
            await ClearAsync().FastPath();
            if (QueuedEvents[0] != null)
                await QueuedEvents[0].ZeroManagedAsync<object>(zero: true).FastPath();
            //if (QueuedEvents[1] != null)
            //    await QueuedEvents[1].ZeroManagedAsync<object>(zero: false).FastPath();
        }
    }
}
