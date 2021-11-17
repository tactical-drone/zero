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
        private static bool Zc = false;
        private readonly ILogger<AutoPeeringEventService> _logger;
        private static IoQueue<AutoPeerEvent>[] _queuedEvents =
        {
            //TODO tuning
            new IoQueue<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", EventBatchSize * TotalBatches, 1000),
            new IoQueue<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", EventBatchSize * TotalBatches, 1000)
        };

        private static volatile int _operational = 1;
        private static long _seq;
        private static volatile int _curIdx;
        public static bool Operational => _operational > 0;
        

        public override async Task<EventResponse> Next(NullMsg request, ServerCallContext context)
        {
            var response = new EventResponse();
            try
            {
                if (_operational == 0)
                    return response;

                var curQ = _queuedEvents[(Interlocked.Increment(ref _curIdx) - 1) % 2];

                if (curQ.Count == 0)
                    return null;

                int c = 0;
                AutoPeerEvent cur;
                while (c < EventBatchSize && (cur = await curQ.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
                {
                    response.Events.Add(cur);
                    c++;
                }

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
                if (_operational > 0 && _queuedEvents[_curIdx%2].Count < EventBatchSize  * TotalBatches)
                {
                    newAutoPeerEvent.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    newAutoPeerEvent.Seq = Interlocked.Increment(ref _seq) - 1;
                    await _queuedEvents[_curIdx % 2].EnqueueAsync(newAutoPeerEvent).FastPath().ConfigureAwait(Zc);
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
            await q[0].ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);
            await q[1].ZeroManagedAsync<object>().FastPath().ConfigureAwait(Zc);
        }
    }
}
