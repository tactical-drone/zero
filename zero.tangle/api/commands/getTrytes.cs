using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getTrytes : IoItApiCommand
    {
        public List<string> hashes;

        public getTrytes() : base(nameof(getTrytes))
        {
        }

        public class Response
        {
            public List<string> trytes;
        }
    }
}