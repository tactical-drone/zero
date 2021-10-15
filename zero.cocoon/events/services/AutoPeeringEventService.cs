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
        private const int TotalBatches = 100;
        private readonly ILogger<AutoPeeringEventService> _logger;
        private static IoQueue<AutoPeerEvent>[] _queuedEvents =
        {
            //TODO tuning
            new IoQueue<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", EventBatchSize * TotalBatches, 2000, disablePressure:false),
            new IoQueue<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", EventBatchSize * TotalBatches, 2000, disablePressure:false)
        };

        private static volatile int _operational = 1;
        private static long _seq;
        private static volatile uint _curIdx;
        

        public override async Task<EventResponse> Next(NullMsg request, ServerCallContext context)
        {
            var response = new EventResponse();
            var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            try
            {
                if (_operational == 0)
                    return response;

                //block a bit on empty queues
                if (_queuedEvents[_curIdx%2].Count == 0)
                    await Task.Delay(200).ConfigureAwait(false);

                int c = 0;
                
                var curQ = _queuedEvents[(Interlocked.Increment(ref _curIdx)-1)% 2];
                var cur = curQ.Head;
            
                while (c++ < EventBatchSize && curQ.Count > 0 && cur != null)
                {
                    response.Events.Add(cur.Value);

                    var tmp = cur.Prev;
                    cur.Prev = null;
                    cur.Next = null;
                    cur.Value = default;

                    cur = tmp;
                }

                if(cur == null)
                    await curQ.ClearAsync().FastPath().ConfigureAwait(false);
                else
                    curQ.ResetTail(cur, c-1);
                
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e);
            }

            return response;
        }

        public static async ValueTask AddEventAsync(AutoPeerEvent newAutoPeerEvent)
        {
            try
            {
                if (_operational > 0 || _queuedEvents[_curIdx%2].Count < EventBatchSize  * TotalBatches)
                {
                    newAutoPeerEvent.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    newAutoPeerEvent.Seq = Interlocked.Increment(ref _seq) - 1;
                    await _queuedEvents[_curIdx % 2].EnqueueAsync(newAutoPeerEvent).FastPath().ConfigureAwait(false);
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
            var q = _queuedEvents;
            _queuedEvents = null;
            if (_queuedEvents == null)
                return;
            await q[0].ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
            await q[1].ZeroManagedAsync<object>().FastPath().ConfigureAwait(false);
        }
    }
}
