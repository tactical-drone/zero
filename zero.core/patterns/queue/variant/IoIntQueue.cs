using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.patterns.queue.variant
{
    public class IoIntQueue: IoQueue<int>
    {
        public IoIntQueue(string description, int capacity, int concurrencyLevel, int initialCount = 1) : base(description, capacity, concurrencyLevel)
        {
        }
    }
}
