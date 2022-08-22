namespace zero.core.patterns.queue.variant
{
    public class IoIntQueue: IoQueue<int>
    {
        public IoIntQueue(string description, int capacity, int concurrencyLevel) : base(description, capacity, concurrencyLevel)
        {
        }
    }
}
