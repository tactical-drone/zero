﻿using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class getInclusionStates : ApiCommand
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