using System;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public class IoZeroSub
    {
        public Func<IIoNanite, Task> Action;
        public volatile bool Schedule;
    }
}