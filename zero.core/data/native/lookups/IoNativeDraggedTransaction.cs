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
        public long attachment_timestamp;

        [ClusteringKey(2)]
        public string Tag;
        
        [ClusteringKey(3)]
        public long timestamp;        

        [ClusteringKey(4)]
        public long attachment_timestamp_lower;

        [ClusteringKey(5)]
        public long attachment_timestamp_upper;
                
        [ClusteringKey(6)] 
        public string Address;        
    }
}
