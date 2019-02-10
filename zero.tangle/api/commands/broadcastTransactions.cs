using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
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