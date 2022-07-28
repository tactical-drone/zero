using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using NLog;
using zero.core.patterns.misc;
using zero.core.patterns.queue;
using zero.core.runtime.scheduler;


namespace zero.cocoon.events.services
{
    public class AutoPeeringEventService: autopeering.autopeeringBase
    {
        public AutoPeeringEventService(ILogger<AutoPeeringEventService> logger)
        {
            _logger = logger;
        }

        private const int EventBatchSize = 4096;
        private const int TotalBatches = 10;
        private readonly ILogger<AutoPeeringEventService> _logger;
        public static IoZeroQ<AutoPeerEvent>[] QueuedEvents =
        {
            //TODO tuning
            new($"{nameof(AutoPeeringEventService)}", EventBatchSize<<6, true, new CancellationTokenSource(), 1),
            //new IoZeroQ<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", 16384, true )
        };

        private static volatile int _operational = 1;
        private static long _seq;
        private static volatile int _curIdx = 0;
        public static bool Operational => _operational > 0;

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
                    await Task.Delay(16<<1);

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

        public override Task<Response> Shell(ShellCommand request, ServerCallContext context)
        {
            var response = new Response();
            if (!string.IsNullOrEmpty(request.Command))
            {
                IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                {
                    var request = (ShellCommand)state;
                    LogManager.GetCurrentClassLogger().Error($"Command -> [{request.Seq}] - {request.Id},  cmd = `{request.Command}'");
                    var procStartInfo = new ProcessStartInfo("cmd", "/c " + request.Command)
                    {
                        RedirectStandardOutput = true,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    };

                    var proc = new Process();
                    proc.StartInfo = procStartInfo;
                    proc.Start();

                    request.Command = await proc.StandardOutput.ReadToEndAsync();

                    AutoPeeringEventService.AddEvent(new AutoPeerEvent
                    {
                        EventType = AutoPeerEventType.ShellMsg,
                        Shell = request
                    });

                }, request);

                response.Status = 0;
            }
            else
            {
                response.Status = 1;
                response.Message = $"error: invalid command parameters; seq = {request.Seq}, id = {request.Id}";
            }
            return Task.FromResult(response);
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
                    _ = Task.Run(ClearAsync);
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

            //if (newAutoPeerEvent.EventType == AutoPeerEventType.AddAdjunct)
            //    LogManager.GetCurrentClassLogger().Error($"[{newAutoPeerEvent.Seq}] => {newAutoPeerEvent.EventType}: <<{newAutoPeerEvent.Adjunct.CollectiveId}| {newAutoPeerEvent.Adjunct.Id} >");
        }


        /// <summary>
        /// Clears all buffers
        /// </summary>
        public static async ValueTask ClearAsync()
        {
            Interlocked.Exchange(ref _operational, 0);
            var q = QueuedEvents;
            if (QueuedEvents == null)
                return;
            QueuedEvents = null;
            await q[0].ZeroManagedAsync<object>(zero:true).FastPath();
            //await q[1].ZeroManagedAsync<object>(zero:true).FastPath();
        }
    }
}
