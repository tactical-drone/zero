using Cassandra.Mapping.Attributes;

namespace zero.core.data.native.lookups
{
    [Table("transaction")]
    public class IoNativeHashedBundle
    {
        [Column(nameof(Hash)), PartitionKey]
        public string Hash { get; set; }

        [Column(nameof(Bundle)), ClusteringKey]
        public string Bundle { get; set; }
    }
}
