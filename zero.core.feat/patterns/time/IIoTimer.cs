using System;
using System.Threading.Tasks;

namespace zero.core.feat.patterns.time
{
    public interface IIoTimer
    {
        ValueTask<int> TickAsync();
    }
}
