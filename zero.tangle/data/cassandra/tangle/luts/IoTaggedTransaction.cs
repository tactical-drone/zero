using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds transactions by tag
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class IoTaggedTransaction<TKey> : IoCassandraPartitionedLut
    {        
        public override long PartitionSize => 60000;
        public TKey Tag { get; set; }

        public long Timestamp { get; set; }

        public TKey Hash { get; set; }

        public TKey Bundle { get; set; }                
    }
}
