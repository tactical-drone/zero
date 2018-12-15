using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("dragnet")]
    public class IoNativeDraggedTransaction
    {
        [PartitionKey]
        public string Hash { get; set; }

        [PartitionKey(1)]
        public string Uri { get; set; }

        [ClusteringKey(0)]
        public short Size;

        [ClusteringKey(1)]
        public long Value;

        [ClusteringKey(2)]
        public long attachment_timestamp;

        [ClusteringKey(3)]
        public string Tag;
        
        [ClusteringKey(4)]
        public long timestamp;        

        [ClusteringKey(5)]
        public long attachment_timestamp_lower;

        [ClusteringKey(6)]
        public long attachment_timestamp_upper;
                
        [ClusteringKey(7)] 
        public string Address;        
    }
}
