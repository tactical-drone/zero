namespace zero.tangle.data.cassandra.tangle.luts
{        
    /// <summary>
    /// Gets the bundle from transaction hash
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class IoBundledHash<TKey>
    {        
        public TKey Hash { get; set; }
        
        public TKey Bundle { get; set; }

        public long Timestamp { get; set; }
        
    }
}
