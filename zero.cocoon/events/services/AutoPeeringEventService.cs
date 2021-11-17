﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using NLog;
using Tangle.Net.Repository.Client;
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

        private const int EventBatchSize = 500;
        private const int TotalBatches = 1000;
        private static bool Zc = false;
        private readonly ILogger<AutoPeeringEventService> _logger;
        private static IoQueue<AutoPeerEvent>[] _queuedEvents =
        {
            //TODO tuning
            new IoQueue<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", EventBatchSize * TotalBatches, 10000),
            new IoQueue<AutoPeerEvent>($"{nameof(AutoPeeringEventService)}", EventBatchSize * TotalBatches, 10000)
        };

        private static volatile int _operational = 1;
        private static long _seq;
        private static volatile int _curIdx;
        public static bool Operational => _operational > 0;

        public static void ToggleActive()
        {
            _operational = _operational > 0 ? 0 : 1;
        }

        public override async Task<EventResponse> Next(NullMsg request, ServerCallContext context)
        {
            var response = new EventResponse();

            if (_operational == 0)
                return response;

            try
            {
                //IoQueue<AutoPeerEvent> curQ = _queuedEvents[(Interlocked.Increment(ref _curIdx) - 1) % 2];
                IoQueue<AutoPeerEvent> curQ = _queuedEvents[0];

                int c = 0;

                while (curQ.Count == 0 && c++ < 10)
                    await Task.Delay(200).ConfigureAwait(Zc);

                c = 0;
                AutoPeerEvent cur;
                while (c < EventBatchSize && (cur = await curQ.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
                //while ((cur = await curQ.DequeueAsync().FastPath().ConfigureAwait(Zc)) != null)
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

                //await curQ.ClipAsync(cur).FastPath().ConfigureAwait(Zc);

                //Console.WriteLine($"c = {c}");
                //await curQ.ClearAsync().FastPath().ConfigureAwait(Zc);
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

        public static async ValueTask AddEventAsync(AutoPeerEvent newAutoPeerEvent)
        {
            try
            {
                var curQ = _queuedEvents[_curIdx % 2];
                if (_operational > 0 && curQ.Count < EventBatchSize  * TotalBatches)
                {
                    newAutoPeerEvent.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    newAutoPeerEvent.Seq = Interlocked.Increment(ref _seq) - 1;
                    await curQ.PushAsync(newAutoPeerEvent).FastPath().ConfigureAwait(Zc);
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
