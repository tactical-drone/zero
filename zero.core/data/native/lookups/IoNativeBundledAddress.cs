using Cassandra.Mapping.Attributes;

namespace zero.core.data.native.lookups
{
    [Table("address")]
    public class IoNativeBundledAddress
    {
        [Column(nameof(Address)), PartitionKey]
        public string Address { get; set; }

        [Column(nameof(Bundle)), PartitionKey(1)]
        public string Bundle { get; set; }
    }
}
