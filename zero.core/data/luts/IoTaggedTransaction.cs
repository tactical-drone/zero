namespace zero.core.data.luts
{    
    /// <summary>
    /// Finds transactions by tag
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoTaggedTransaction<TBlob>
    {                
        public TBlob Tag { get; set; }

        public TBlob ObsoleteTag { get; set; }

        public TBlob Hash { get; set; }

        public long Timestamp { get; set; }
    }
}
