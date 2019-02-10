namespace zero.tangle.data.cassandra.tangle.luts
{        
    /// <summary>
    /// Gets the bundle from transaction hash
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoBundledHash<TBlob>
    {        
        public TBlob Hash { get; set; }
        
        public TBlob Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
