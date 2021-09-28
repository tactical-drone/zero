using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Org.BouncyCastle.Bcpg.OpenPgp;


namespace zero.cocoon.events.services
{
    public class AutoPeeringEventService: autopeering.autopeeringBase
    {
        public AutoPeeringEventService(ILogger<AutoPeeringEventService> logger)
        {
            _logger = logger;
        }

        private readonly ILogger<AutoPeeringEventService> _logger;
        private static ConcurrentQueue<AutoPeerEvent> QueuedEvents = new ConcurrentQueue<AutoPeerEvent>();
        private static volatile int _operational = 1;
        private static long _seq;
        private const int EventBatchSize = 10000;

        public override Task<EventResponse> Next(NullMsg request, ServerCallContext context)
        {
            var response = new EventResponse();

            if (_operational == 0)
                return Task.FromResult(response);

            var c = 0;
            while (c++ < EventBatchSize && QueuedEvents.TryDequeue(out var netEvent))
            {
                response.Events.Add(netEvent);
            }
            
            return Task.FromResult(response);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AddEvent(AutoPeerEvent newAutoPeerEvent)
        {
            //return;
            try
            {
                if (_operational > 0 || QueuedEvents.Count < 100000)
                {
                    newAutoPeerEvent.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    newAutoPeerEvent.Seq = Interlocked.Increment(ref _seq) - 1;
                    QueuedEvents.Enqueue(newAutoPeerEvent);
                }
            }
            catch (NullReferenceException)
            {
                
            }
        }


        /// <summary>
        /// Clears all buffers
        /// </summary>
        public static void Clear()
        {
            var q = QueuedEvents;
            QueuedEvents = null;
            if(QueuedEvents == null)
                return;
            q.Clear();
            Interlocked.Exchange(ref _operational, 0);
        }
    }
}
