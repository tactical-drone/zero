using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
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