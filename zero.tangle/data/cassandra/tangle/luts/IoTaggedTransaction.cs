namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds transactions by tag
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoTaggedTransaction<TBlob>
    {
        public long Partition;
        public TBlob Tag { get; set; }

        public TBlob ObsoleteTag { get; set; }

        public TBlob Hash { get; set; }

        public TBlob Bundle { get; set; }        

        public long Timestamp { get; set; }

        public bool IsMilestoneTransaction { get; set; }

        public long MilestoneIndex { get; set; }
    }
}
