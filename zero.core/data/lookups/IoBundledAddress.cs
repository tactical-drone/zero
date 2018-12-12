using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    public class IoBundledAddress
    {
        [Column(nameof(Address)), PartitionKey]
        public sbyte[] Address { get; set; }

        [Column(nameof(Bundle)), ClusteringKey]
        public sbyte[] Bundle { get; set; }
    }
}
