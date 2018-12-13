using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.native.lookups
{
    [Table("tag")]
    public class IoNativeTaggedTransaction
    {
        [Column(nameof(Tag)), PartitionKey]        
        public string Tag { get; set; }

        [Column(nameof(Hash))]
        public string Hash { get; set; }
    }
}
