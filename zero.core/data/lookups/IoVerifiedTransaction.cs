using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("verifier")]
    public class IoVerifiedTransaction
    {
        [PartitionKey]
        [Frozen]
        public byte[] Hash { get; set; }

        [ClusteringKey]
        public sbyte Pow { get; set; }

        [Column(nameof(Verifier)), PartitionKey(1)]
        [Frozen]
        public byte[] Verifier { get; set; }        
    }
}
