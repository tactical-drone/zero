using System;
using zero.interop.entangled.common.model.abstraction;

namespace zero.interop.entangled.common.model.native
{
    public class IoNativeTransactionModel : IIoInteropTransactionModel
    {
        public string Address { get; set; }
        public string Message { get; set; }
        public Int64 Value { get; set; }
        public string ObsoleteTag { get; set; }
        public Int64 Timestamp { get; set; }
        public Int64 CurrentIndex { get; set; }
        public Int64 LastIndex { get; set; }
        public string Bundle { get; set; }
        public string Trunk { get; set; }
        public string Branch { get; set; }
        public string Tag { get; set; }
        public Int64 AttachmentTimestamp { get; set; }
        public Int64 AttachmentTimestampLower { get; set; }
        public Int64 AttachmentTimestampUpper { get; set; }
        public string Nonce { get; set; }
        public string Hash { get; set; }
        public UInt64 SnapshotIndex { get; set; }
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
