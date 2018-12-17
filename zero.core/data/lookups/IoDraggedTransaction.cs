using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    [Table("dragnet")]
    public class IoDraggedTransaction
    {                
        [PartitionKey]
        public byte[] Address { get; set; }

        [PartitionKey(1)]
        public byte[] CounterParty { get; set; }

        [ClusteringKey]
        public long timestamp;

        [ClusteringKey]
        public long attachment_timestamp;

        [ClusteringKey]
        public long Value;
                                        
        [ClusteringKey(7)]
        public short Quality;

        [ClusteringKey(1)]
        public string Uri { get; set; }

        public float BtcValue;
        public float EthValue;
        public float EurValue;
        public float UsdValue;
    }
}
