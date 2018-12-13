using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("tag")]
    public class IoTaggedTransaction
    {
        [Column(nameof(Tag)), PartitionKey]
        [Frozen]
        public byte[] Tag { get; set; }

        [Column(nameof(Hash))]        
        public byte[] Hash { get; set; }
    }
}
