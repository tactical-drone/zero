using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.patterns.queue.variant
{
    public class IoIntQueue: IoQueue<int>
    {
        public IoIntQueue(string description, int capacity, int concurrencyLevel, int initialCount = 0, bool enableBackPressure = false, bool disablePressure = true, bool autoScale = false) : base(description, capacity, concurrencyLevel, initialCount, enableBackPressure, disablePressure, autoScale)
        {
        }
    }
}
