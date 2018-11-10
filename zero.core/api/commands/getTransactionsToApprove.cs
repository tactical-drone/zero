using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getTransactionsToApprove : ApiCommand
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