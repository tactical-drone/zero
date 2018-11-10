using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getBalances : ApiCommand
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