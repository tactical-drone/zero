using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds transactions by tag
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoTaggedTransaction<TBlob> : IoCassandraPartitionedLut
    {        
        public override long PartitionSize => 60000;
        public TBlob Tag { get; set; }

        public long Timestamp { get; set; }

        public TBlob Hash { get; set; }

        public TBlob Bundle { get; set; }                
    }
}
