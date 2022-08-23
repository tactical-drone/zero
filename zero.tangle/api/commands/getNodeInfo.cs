using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Threading.Tasks;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getNodeInfo : IoItApiCommand
    {
        public getNodeInfo() : base(nameof(getNodeInfo))
        {
        }

#pragma warning disable 1998
        public override async Task<ResponseBase> ProcessCommandAsync(JsonDocument jsonCommandobJObject)
#pragma warning restore 1998
        {
            var response = new Response
            {
                appName = "unimatrix zero",
                appVersion = "v0.1.0",
                duration = 0,
                jreAvailableProcessors = (ushort) Environment.ProcessorCount,
                jreFreeMemory = (ulong) System.Diagnostics.Process.GetCurrentProcess().WorkingSet64 -
                                (ulong) GC.GetTotalMemory(false),
                jreMaxMemory = (ulong) System.Diagnostics.Process.GetCurrentProcess().WorkingSet64,
                jreTotalMemory = (ulong) GC.GetTotalMemory(false),
                latestMilestone = "0",
                latestMilestoneIndex = 0,
                latestSolidSubtangleMilestone = "0",
                latestSolidSubtangleMilestoneIndex = 0,
                neighbors = 0,
                packetsQueueSize = 0,
                time = (ulong) DateTime.Now.Ticks,
                tips = 0,
                transactionsToRequest = 0
            };
            return response;
        }

        public class Response : ResponseBase
        {
            public string appName;
            public string appVersion;
            public ulong duration;
            public ushort jreAvailableProcessors;
            public ulong jreFreeMemory;
            public ulong jreMaxMemory;
            public ulong jreTotalMemory;
            public string latestMilestone;
            public ulong latestMilestoneIndex;
            public string latestSolidSubtangleMilestone;
            public ulong latestSolidSubtangleMilestoneIndex;
            public ushort neighbors;
            public uint packetsQueueSize;
            public ulong time;
            public ulong tips;
            public ulong transactionsToRequest;
        }
    }
}