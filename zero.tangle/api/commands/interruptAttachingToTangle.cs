using System.Diagnostics.CodeAnalysis;
using zero.core.feat.api.commands;

namespace zero.tangle.api.commands
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public class interruptAttachingToTangle : IoItApiCommand
    {
        public interruptAttachingToTangle() : base(nameof(interruptAttachingToTangle))
        {
        }
    }
}