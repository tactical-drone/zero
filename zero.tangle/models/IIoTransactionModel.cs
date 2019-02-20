using System;
using System.Runtime.Serialization;

// ReSharper disable InconsistentNaming
namespace zero.core.models
{
    /// <summary>
    /// The transaction model contract used throughout the system
    /// </summary>    
    public interface IIoTransactionModel<TKey> : IIoTransactionModelInterface
    {
        [IgnoreDataMember]
        ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }

        [DataMember]
        TKey SignatureOrMessage { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> AddressBuffer { get; set; }

        [DataMember]
        TKey Address { get; set; }

        [DataMember]
        long Value { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> ObsoleteTagBuffer { get; set; }

        [DataMember]
        TKey ObsoleteTag { get; set; }

        [DataMember]
        long Timestamp { get; set; }

        [DataMember]
        long CurrentIndex { get; set; }

        [DataMember]
        long LastIndex { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> BundleBuffer { get; set; }

        [DataMember]
        TKey Bundle { get; set; }
    
        [IgnoreDataMember]
        ReadOnlyMemory<byte> TrunkBuffer { get; set; }

        [DataMember]
        TKey Trunk { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> BranchBuffer { get; set; }

        [DataMember]
        TKey Branch { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> TagBuffer { get; set; }

        [DataMember]
        TKey Tag { get; set; }

        [DataMember]
        long AttachmentTimestamp { get; set; }

        [DataMember]
        long AttachmentTimestampLower { get; set; }

        [DataMember]
        long AttachmentTimestampUpper { get; set; }

        [IgnoreDataMember]
        ReadOnlyMemory<byte> NonceBuffer { get; set; }

        [DataMember]
        TKey Nonce { get; set; }
    
        [IgnoreDataMember]
        ReadOnlyMemory<byte> HashBuffer { get; set; }

        [DataMember]
        TKey Hash { get; set; }

        [DataMember]
        TKey Snapshot { get; set; }

        [DataMember]
        long MilestoneIndexEstimate { get; set; }

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
        
        [DataMember]
        bool IsMilestoneTransaction { get; set; }

        [IgnoreDataMember]
        long ConfirmationTime { get; set; }

        [IgnoreDataMember]
        IIoTransactionModel<TKey> MilestoneEstimateTransaction { get; set; }
        
        string AsTrytes(ReadOnlyMemory<byte> field, int fixedLenTritsToConvert = 0);
    
        ReadOnlyMemory<byte> AsBlob();
        long GetMilestoneIndex();

        long GetAttachmentTime();
    }
}
