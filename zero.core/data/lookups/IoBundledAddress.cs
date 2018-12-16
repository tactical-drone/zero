using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("Address")]
    public class IoBundledAddress
    {
        [Column(nameof(Address)), PartitionKey]
        [Frozen]
        public sbyte[] Address { get; set; }

        [Column(nameof(Bundle)), PartitionKey(1)]
        [Frozen]
        public byte[] Bundle { get; set; }
    }
}
