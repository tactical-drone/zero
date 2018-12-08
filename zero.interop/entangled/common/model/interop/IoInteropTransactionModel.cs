using System;
using System.Runtime.Serialization;

namespace zero.interop.entangled.common.model.abstraction
{
    public interface IIoInteropTransactionModel
    {
        [DataMember]
        string Message { get; set; }

        [DataMember]
        string Address { get; set; }

        [DataMember]
        Int64 Value { get; set; }

        [DataMember]
        string ObsoleteTag { get; set; }

        [DataMember]
        Int64 Timestamp { get; set; }

        [DataMember]
        Int64 CurrentIndex { get; set; }

        [DataMember]
        Int64 LastIndex { get; set; }

        [DataMember]
        string Bundle { get; set; }

        [DataMember]
        string Trunk { get; set; }

        [DataMember]
        string Branch { get; set; }

        [DataMember]
        string Tag { get; set; }

        [DataMember]
        Int64 AttachmentTimestamp { get; set; }

        [DataMember]
        Int64 AttachmentTimestampLower { get; set; }

        [DataMember]
        Int64 AttachmentTimestampUpper { get; set; }

        [DataMember]
        string Nonce { get; set; }

        [DataMember]
        string Hash { get; set; }

        [DataMember]
        UInt64 SnapshotIndex { get; set; }

        [DataMember]
        bool Solid { get; set; }

        [DataMember]
        int Pow { get; set; }

        [DataMember]
        string Color { get; }
    }
}
