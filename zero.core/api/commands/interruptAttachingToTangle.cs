﻿using System.Diagnostics.CodeAnalysis;

namespace zero.core.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class interruptAttachingToTangle : IoItApiCommand
    {
        public interruptAttachingToTangle() : base(nameof(interruptAttachingToTangle))
        {
        }
    }
}