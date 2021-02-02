using System;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public class IoZeroSub
    {
        public Func<IIoNanite, ValueTask> Action;
        public volatile bool Schedule;
    }
}