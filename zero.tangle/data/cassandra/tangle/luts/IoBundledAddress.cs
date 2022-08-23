namespace zero.tangle.data.cassandra.tangle.luts
{
    /// <summary>
    /// Gets the bundle from address
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class IoBundledAddress<TKey>
    {                
        public TKey Address { get; set; }
        
        public TKey Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
