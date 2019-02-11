namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds a transaction's verifier
    /// </summary>
    /// <typeparam name="TBlob">Type of blob field</typeparam>
    public class IoApprovedTransaction<TBlob>
    {
        public TBlob Hash { get; set; }
        public TBlob Verifier { get; set; }                
        public sbyte Pow { get; set; }
        public long Partition { get; set; }
        public long MilestoneIndexEstimate { get; set; }
    }
}
