﻿using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.native
{
    [Table("bundle")]
    public class IoNativeTransactionModel : IIoInteropTransactionModel
    {
        [Ignore]
        public IoMarshalledTransaction TrimmedMap { get; set; }

        [Ignore]
        public IoMarshalledTransaction RawMapping { get; set; }

        [PartitionKey(1)]
        public string Address { get; set; }

        [Column(nameof(IoMarshalledTransaction.signature_or_message))]        
        public string SignatureOrMessage { get; set; }

        [ClusteringKey(2)]
        public long Value { get; set; }

        [Column(nameof(IoMarshalledTransaction.obsolete_tag))]
        public string ObsoleteTag { get; set; }

        [ClusteringKey(9)]
        public long Timestamp { get; set; }

        [Column(nameof(IoMarshalledTransaction.current_index)), ClusteringKey(1)]
        public long CurrentIndex { get; set; }

        [Column(nameof(IoMarshalledTransaction.last_index)), ClusteringKey(0)]
        public long LastIndex { get; set; }

        [Column(nameof(Bundle)), PartitionKey(0)]
        public string Bundle { get; set; }

        public string Trunk { get; set; }
        public string Branch { get; set; }

        [ClusteringKey(3)]
        public string Tag { get; set; }

        [Column(nameof(IoMarshalledTransaction.attachment_timestamp)),ClusteringKey(5)]
        public long AttachmentTimestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_lower)), ClusteringKey(6)]
        public long AttachmentTimestampLower { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_upper)), ClusteringKey(7)]
        public long AttachmentTimestampUpper { get; set; }
        public string Nonce { get; set; }

        [ClusteringKey(4)]
        public string Hash { get; set; }
        
        [Ignore]
        public long SnapshotIndex { get; set; }

        [Ignore]
        public bool Solid { get; set; }

        [Ignore]
        public sbyte Pow { get; set; }

        [Ignore]
        public sbyte FakePow { get; set; }

        [Ignore]
        public string Color
        {
            get
            {
                if (Pow == 0)
                    return "color: red";
                return Pow < 0 ? "color: orange" : "color:green";
            }
        }

        [Ignore]
        public string Uri { get; set; }

        [Ignore] //TODO config
        public string Body
        {
            get => SignatureOrMessage.Trim('9');
            set { }
        }
        
        public short Size { get; set; }
    }
}
