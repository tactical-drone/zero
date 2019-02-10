using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using zero.core.api.commands;

namespace zero.tangle.api.commands
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