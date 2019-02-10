using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class findTransactions : IoItApiCommand
    {
        public List<string> addresses;
        public List<string> approvees;

        public List<string> bundles;
        public List<string> tags;

        public findTransactions() : base(nameof(findTransactions))
        {
        }

        public class Response
        {
            public List<string> addresses;
            public List<string> approvees;
            public List<string> bundles;
            public List<string> tags;
        }
    }
}