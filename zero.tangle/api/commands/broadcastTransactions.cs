using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class broadcastTransactions : IoItApiCommand
    {
        public List<string> trytes;

        public broadcastTransactions() : base(nameof(broadcastTransactions))
        {
        }
    }
}