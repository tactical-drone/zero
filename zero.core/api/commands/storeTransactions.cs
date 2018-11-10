using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class storeTransactions : ApiCommand
    {
        public List<string> trytes;

        public storeTransactions() : base(nameof(storeTransactions))
        {
        }
    }
}