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

        [ClusteringKey(0)]
        public string Uri { get; set; }

        [ClusteringKey(1)]
        public int Size;

        [ClusteringKey(2)]
        public long Value;

        [ClusteringKey(3)]
        public long attachment_timestamp;

        [ClusteringKey(4)]
        public string Tag;
        
        [ClusteringKey(5)]
        public long timestamp;        

        [ClusteringKey(6)]
        public long attachment_timestamp_lower;

        [ClusteringKey(7)]
        public long attachment_timestamp_upper;
                
        [ClusteringKey(8)] 
        public string Address;        
    }
}
