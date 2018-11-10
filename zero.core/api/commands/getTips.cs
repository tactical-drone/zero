using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getTips : ApiCommand
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