using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds a transaction's verifier
    /// </summary>
    /// <typeparam name="TBlob">Type of blob field</typeparam>
    public class IoApprovedTransaction<TBlob>:IoCassandraPartitionedLut
    {
        public override long PartitionSize => 600000;
        public TBlob Hash { get; set; }
        public TBlob Verifier { get; set; }
        public TBlob TrunkBranch { get; set; }
        public long Balance { get; set; }
        public sbyte Pow { get; set; }
        public long Timestamp { get; set; }
        public long SecondsToMilestone { get; set; }
        public long MilestoneIndexEstimate { get; set; }        
    }
}
