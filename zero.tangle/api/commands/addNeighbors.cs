using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class addNeighbors : IoItApiCommand
    {
        public List<string> uris;

        public addNeighbors() : base(nameof(addNeighbors))
        {
        }

        public class Response
        {
            public ushort addNeighbors;
            public ulong duration;
        }
    }
}