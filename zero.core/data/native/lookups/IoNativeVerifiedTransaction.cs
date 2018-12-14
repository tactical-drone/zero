using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("verifier")]
    public class IoNativeVerifiedTransaction
    {
        [PartitionKey]        
        public string Hash { get; set; }

        [ClusteringKey(0)]
        public int Pow { get; set; }

        [Column(nameof(Verifier)), ClusteringKey(1)]        
        public string Verifier { get; set; }        
    }
}
