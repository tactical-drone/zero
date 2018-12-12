using System;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.native
{
    [Table("NativeBundle")]
    public class IoNativeTransactionModel : IIoInteropTransactionModel
    {
        public string Address { get; set; }
        public string Message { get; set; }
        public long Value { get; set; }
        public string ObsoleteTag { get; set; }
        public long Timestamp { get; set; }
        [Column(nameof(CurrentIndex)), ClusteringKey]
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
        [Column(nameof(Bundle)), PartitionKey]
        public string Bundle { get; set; }
        public string Trunk { get; set; }
        public string Branch { get; set; }
        public string Tag { get; set; }
        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public string Nonce { get; set; }
        public string Hash { get; set; }
        public long SnapshotIndex { get; set; }
        public bool Solid { get; set; }
        public int Pow { get; set; }
        public int FakePow { get; set; }
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
