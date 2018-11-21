using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getTransactionsToApprove : IoItApiCommand
    {
        public uint depth;

        public getTransactionsToApprove() : base(nameof(getTransactionsToApprove))
        {
        }

        public class Response
        {
            public string branchTransaction;
            public ulong duration;
            public string trunkTransaction;
        }
    }
}