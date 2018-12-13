using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("Address")]
    public class IoBundledAddress
    {
        [Column(nameof(Address)), PartitionKey]
        [Frozen]
        public byte[] Address { get; set; }

        [Column(nameof(Bundle)), ClusteringKey]
        [Frozen]
        public byte[] Bundle { get; set; }
    }
}
