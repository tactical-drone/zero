using System;
using System.Runtime.Serialization;

// ReSharper disable InconsistentNaming
namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// The transaction model contract used throughout the system
    /// </summary>
    /// <typeparam name="ReadOnlyMemory<byte>"></typeparam>
    public interface IIoTransactionModel    
    {
        [DataMember]
        ReadOnlyMemory<byte> SignatureOrMessage { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Address { get; set; }

        [DataMember]
        long Value { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> ObsoleteTag { get; set; }

        [DataMember]
        long Timestamp { get; set; }

        [DataMember]
        long CurrentIndex { get; set; }

        [DataMember]
        long LastIndex { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Bundle { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Trunk { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Branch { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Tag { get; set; }

        [DataMember]
        long AttachmentTimestamp { get; set; }

        [DataMember]
        long AttachmentTimestampLower { get; set; }

        [DataMember]
        long AttachmentTimestampUpper { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Nonce { get; set; }

        [DataMember]
        ReadOnlyMemory<byte> Hash { get; set; }

        [DataMember]
        long SnapshotIndex { get; set; }

        [DataMember]
        bool Solid { get; set; }

        [DataMember]
        sbyte Pow { get; set; }

        [DataMember]
        sbyte ReqPow { get; set; }

        [DataMember]
        string Color { get; }

        [DataMember]
        string Uri { get; set; }        

        [DataMember]
        short Size { get; set; }        
        
        string AsTrytes(ReadOnlyMemory<byte> field, int fixedLenTritsToConvert = 0);
    
        ReadOnlyMemory<byte> AsBlob();
    }
}
