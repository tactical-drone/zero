using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.native.lookups
{
    [Table("dragnet")]
    public class IoNativeDraggedTransaction
    {
        [PartitionKey]
        public string Bundle { get; set; }

        [PartitionKey(1)]
        public long LastIndex { get; set; }

        [ClusteringKey(0, SortOrder.Descending)]
        public long attachment_timestamp;

        [ClusteringKey(1, SortOrder.Descending)]
        public long timestamp;
                                
        [ClusteringKey(2, SortOrder.Descending)]
        public long Value;

        [ClusteringKey(4)]
        public short Quality;

        [ClusteringKey(5)]
        public float BtcValue;
        [ClusteringKey(6)]
        public float EthValue;
        [ClusteringKey(7)]
        public float EurValue;
        [ClusteringKey(8)]
        public float UsdValue;
        
        [ClusteringKey(9)]
        public string Uri { get; set; }
    }
}
