using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.native
{
    [Table("bundle")]
    public class IoNativeTransactionModel : IIoInteropTransactionModel
    {
        [Ignore]
        public IoMarshalledTransaction Mapping { get; set; }
        public string Address { get; set; }
        [Column(nameof(IoMarshalledTransaction.signature_or_message))]
        public string Message { get; set; }
        public long Value { get; set; }
        [Column(nameof(IoMarshalledTransaction.obsolete_tag))]
        public string ObsoleteTag { get; set; }
        public long Timestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.current_index)), ClusteringKey]
        public long CurrentIndex { get; set; }
        [Column(nameof(IoMarshalledTransaction.last_index))]
        public long LastIndex { get; set; }
        [Column(nameof(Bundle)), PartitionKey]
        public string Bundle { get; set; }
        public string Trunk { get; set; }
        public string Branch { get; set; }
        public string Tag { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp))]
        public long AttachmentTimestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_lower))]
        public long AttachmentTimestampLower { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_upper))]
        public long AttachmentTimestampUpper { get; set; }
        public string Nonce { get; set; }
        public string Hash { get; set; }
        
        [Ignore]
        public long SnapshotIndex { get; set; }
        [Ignore]
        public bool Solid { get; set; }
        [Ignore]
        public int Pow { get; set; }
        [Ignore]
        public int FakePow { get; set; }

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
    }
}
