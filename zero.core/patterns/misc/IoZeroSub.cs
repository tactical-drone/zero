using System;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public struct IoZeroSub
    {
        public Func<IIoZeroable, Task> Action;
        public volatile bool Schedule;
    }
}