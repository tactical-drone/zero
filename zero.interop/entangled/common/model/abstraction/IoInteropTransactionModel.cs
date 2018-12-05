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
        long Value { get; set; }

        [DataMember]
        string ObsoleteTag { get; set; }

        [DataMember]
        long Timestamp { get; set; }

        [DataMember]
        long CurrentIndex { get; set; }

        [DataMember]
        long LastIndex { get; set; }

        [DataMember]
        string Bundle { get; set; }

        [DataMember]
        string Trunk { get; set; }

        [DataMember]
        string Branch { get; set; }

        [DataMember]
        string Tag { get; set; }

        [DataMember]
        long AttachmentTimestamp { get; set; }

        [DataMember]
        long AttachmentTimestampLower { get; set; }

        [DataMember]
        long AttachmentTimestampUpper { get; set; }

        [DataMember]
        string Nonce { get; set; }

        [DataMember]
        string Hash { get; set; }

        [DataMember]
        long SnapshotIndex { get; set; }

        [DataMember]
        bool Solid { get; set; }
    }
}
