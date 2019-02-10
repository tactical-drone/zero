using System;
using System.Runtime.Serialization;

// ReSharper disable InconsistentNaming
namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// The transaction model contract used throughout the system
    /// </summary>    
    public interface IIoTransactionModel<TBlob>
    {
        [IgnoreDataMember]
        ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }

        [DataMember]
        TBlob SignatureOrMessage { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> AddressBuffer { get; set; }

        [DataMember]
        TBlob Address { get; set; }

        [DataMember]
        long Value { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> ObsoleteTagBuffer { get; set; }

        [DataMember]
        TBlob ObsoleteTag { get; set; }

        [DataMember]
        long Timestamp { get; set; }

        [DataMember]
        long CurrentIndex { get; set; }

        [DataMember]
        long LastIndex { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> BundleBuffer { get; set; }

        [DataMember]
        TBlob Bundle { get; set; }
    
        [IgnoreDataMember]
        ReadOnlyMemory<byte> TrunkBuffer { get; set; }

        [DataMember]
        TBlob Trunk { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> BranchBuffer { get; set; }

        [DataMember]
        TBlob Branch { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> TagBuffer { get; set; }

        [DataMember]
        TBlob Tag { get; set; }

        [DataMember]
        long AttachmentTimestamp { get; set; }

        [DataMember]
        long AttachmentTimestampLower { get; set; }

        [DataMember]
        long AttachmentTimestampUpper { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> NonceBuffer { get; set; }

        [DataMember]
        TBlob Nonce { get; set; }
    
        [IgnoreDataMember]
        ReadOnlyMemory<byte> HashBuffer { get; set; }

        [DataMember]
        TBlob Hash { get; set; }

        [DataMember]
        TBlob Snapshot { get; set; }

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
