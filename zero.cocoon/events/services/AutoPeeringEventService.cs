using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using NLog;
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

        private const int EventBatchSize = 10000;
        private const int TotalBatches = 200;
        private readonly ILogger<AutoPeeringEventService> _logger;
        public static IoZeroQ<AutoPeerEvent>[] QueuedEvents =
        {
            //TODO tuning
            new IoZeroQ<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", 16384, true ),
            new IoZeroQ<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", 16384, true )
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
                await QueuedEvents[(_curIdx + 1) % 2].ZeroManagedAsync<object>().FastPath();
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

                while (curQ.Count == 0 && c++ < 100)
                    await Task.Delay(50);

                c = 0;
                AutoPeerEvent cur;
                while (c < EventBatchSize && curQ.TryDequeue(out cur))
                {
                    response.Events.Add(cur);
                    c++;
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

        public static void AddEvent(AutoPeerEvent newAutoPeerEvent)
        {
            try
            {
                var curQ = QueuedEvents[_curIdx % 2];
                if (_operational > 0 && curQ.Count < EventBatchSize  * TotalBatches)
                {
                    newAutoPeerEvent.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    newAutoPeerEvent.Seq = Interlocked.Increment(ref _seq) - 1;
                    curQ.TryEnqueue(newAutoPeerEvent);
                }
            }
            catch
            {
                // ignored
            }
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
            await q[1].ZeroManagedAsync<object>(zero:true).FastPath();
        }
    }
}
