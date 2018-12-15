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

        [ClusteringKey(5)]
        public decimal BtcValue;
        [ClusteringKey(6)]
        public decimal EthValue;
        [ClusteringKey(7)]
        public decimal EurValue;
        [ClusteringKey(8)]
        public decimal UsdValue;

        [ClusteringKey(2)]
        public long attachment_timestamp;

        [ClusteringKey(3)]
        public string Tag;
        
        [ClusteringKey(4)]
        public long timestamp;        

        [ClusteringKey(9)]
        public long attachment_timestamp_lower;

        [ClusteringKey(10)]
        public long attachment_timestamp_upper;

        public short Quality;
    }
}
