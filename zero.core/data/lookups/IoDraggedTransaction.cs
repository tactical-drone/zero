﻿using System;
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

        [PartitionKey(1)]
        public string Uri { get; set; }
        
        [ClusteringKey(0)]
        public long Value;

        [ClusteringKey(1)]
        public float BtcValue;
        [ClusteringKey(2)]
        public float EthValue;
        [ClusteringKey(3)]
        public float EurValue;
        [ClusteringKey(4)]
        public float UsdValue;

        [ClusteringKey(5)]
        public short Quality;

        [ClusteringKey(6)]
        public long attachment_timestamp;

        [ClusteringKey(7)] [Frozen]
        public sbyte[] Tag;
        
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
