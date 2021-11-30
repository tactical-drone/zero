using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.patterns.queue.variant
{
    public class IoIntQueue: IoQueue<int>
    {
        public IoIntQueue(string description, int capacity, int concurrencyLevel, bool enableBackPressure = false, bool disablePressure = true, bool autoScale = false) : base(description, capacity, concurrencyLevel, enableBackPressure, disablePressure, autoScale)
        {
        }


    }
}
