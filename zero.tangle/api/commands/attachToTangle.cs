using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class attachToTangle : IoItApiCommand
    {
        public string branchTransaction;
        public uint minWeightMagnitude;

        public string trunkTransaction;
        public List<string> trytes;

        public attachToTangle() : base(nameof(attachToTangle))
        {
        }

        public class Response
        {
            public List<string> trytes;
        }
    }
}