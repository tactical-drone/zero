﻿using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class removeNeighbors : IoItApiCommand
    {
        public List<string> uris;

        public removeNeighbors() : base(nameof(removeNeighbors))
        {
        }

        public class Response
        {
            public ulong duration;
            public ushort removeNeighbors;
        }
    }
}