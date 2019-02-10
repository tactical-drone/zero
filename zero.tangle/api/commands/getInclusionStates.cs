using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getInclusionStates : IoItApiCommand
    {
        public List<string> tips;

        public List<string> transactions;

        public getInclusionStates() : base(nameof(getInclusionStates))
        {
        }

        public class Response
        {
            public ulong duration;
            public List<bool> states;
        }
    }
}