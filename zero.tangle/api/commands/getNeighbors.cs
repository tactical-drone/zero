using System.Diagnostics.CodeAnalysis;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    internal class getNeighbors : IoItApiCommand
    {
        public getNeighbors() : base(nameof(getNeighbors))
        {
        }

        public class Response
        {
            //public ulong duration;
            //public List<Neighbor> neighbors;

            public class Neighbor
            {
                //public string address;
                //public ulong numberOfAllTransactions;
                //public ulong numberOfInvalidTransactions;
                //public ulong numberOfNewTransactions;
            }
        }
    }
}