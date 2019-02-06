namespace zero.core.data.providers.cassandra.keyspaces.tangle.luts
{
    /// <summary>
    /// Gets the bundle from address
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoBundledAddress<TBlob>
    {                
        public TBlob Address { get; set; }
        
        public TBlob Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
