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
        public long Value;

        [ClusteringKey(1)]
        public decimal BtcValue;
        [ClusteringKey(2)]
        public decimal EthValue;
        [ClusteringKey(3)]
        public decimal EurValue;
        [ClusteringKey(4)]
        public decimal UsdValue;

        [ClusteringKey(5)]
        public short Quality;

        [ClusteringKey(6)]
        public long attachment_timestamp;

        [ClusteringKey(7)]
        public string Tag;
        
        [ClusteringKey(8)]
        public long timestamp;        

        [ClusteringKey(9)]
        public long attachment_timestamp_lower;

        [ClusteringKey(10)]
        public long attachment_timestamp_upper;

        [ClusteringKey(11)]
        public short Size;        
    }
}
