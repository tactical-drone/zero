using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getBalances : IoItApiCommand
    {
        public List<string> addresses;
        public uint threshold;

        public getBalances() : base(nameof(getBalances))
        {
        }

        public class Response
        {
            public List<string> balances;
            public ulong duration;
            public string milestone;
            public ulong milestoneIndex;
        }
    }
}