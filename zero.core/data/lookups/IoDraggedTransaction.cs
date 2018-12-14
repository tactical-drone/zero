using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("dragnet")]
    public class IoDraggedTransaction
    {
        [PartitionKey] [Frozen]
        public byte[] Hash { get; set; }

        [ClusteringKey(0)]
        public string Uri { get; set; }

        [ClusteringKey(1)]
        public long Size;

        [ClusteringKey(2)]
        public long Value;

        [ClusteringKey(3)]
        public long attachment_timestamp;

        [ClusteringKey(4)] [Frozen]
        public sbyte[] Tag;
        
        [ClusteringKey(5)]
        public long timestamp;        

        [ClusteringKey(6)]
        public long attachment_timestamp_lower;

        [ClusteringKey(7)]
        public long attachment_timestamp_upper;
                
        [ClusteringKey(8)] [Frozen]
        public byte[] Address;        
    }
}
