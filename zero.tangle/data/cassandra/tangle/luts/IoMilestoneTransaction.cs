using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{
    public class IoMilestoneTransaction<TKey> : IoCassandraPartitionedLut
    {
        public override long PartitionSize => 600000;
        public TKey Hash { get; set; }
        public TKey Bundle { get; set; }
        public TKey ObsoleteTag { get; set; }
        public long Timestamp { get; set; }        
        public long MilestoneIndex { get; set; }
    }
}
