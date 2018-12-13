using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("Transaction")]
    public class IoHashedBundle
    {
        [Column(nameof(Hash)), PartitionKey]
        [Frozen]
        public byte[] Hash { get; set; }

        [Column(nameof(Bundle)), ClusteringKey]
        [Frozen]
        public byte [] Bundle { get; set; }
    }
}
