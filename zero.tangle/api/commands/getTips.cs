using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getTips : IoItApiCommand
    {
        public getTips() : base(nameof(getTips))
        {
        }

        public class Reponse
        {
            public ulong duration;
            public List<string> hashes;
        }
    }
}