using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    public class IoHashedBundle
    {
        [Column(nameof(Hash)), PartitionKey]
        public sbyte[] Hash { get; set; }

        [Column(nameof(Bundle)), ClusteringKey]
        public sbyte [] Bundle { get; set; }
    }
}
