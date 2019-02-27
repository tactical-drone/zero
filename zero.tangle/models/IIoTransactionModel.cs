using System;
using System.Runtime.Serialization;
using zero.core.models;
using zero.interop.entangled.common.model;

// ReSharper disable InconsistentNaming
namespace zero.tangle.models
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
        int ConfirmationTime { get; set; }

        [IgnoreDataMember]
        IIoTransactionModel<TKey> MilestoneEstimateTransaction { get; set; }
        
        string AsTrytes(ReadOnlyMemory<byte> field, int maxFlexTritsToConvert = IoTransaction.NUM_TRITS_HASH, int tryteLen = IoTransaction.NUM_TRYTES_HASH);

        string AsKeyString(ReadOnlyMemory<byte> field, int maxFlexTritsToConvert = IoTransaction.NUM_TRITS_HASH, int tryteLen = IoTransaction.NUM_TRYTES_HASH);

        TKey Trimmed(TKey field, byte nullSet = 9);

        ReadOnlyMemory<byte> AsBlob();

        void PopulateTotalSize();

        long GetMilestoneIndex();

        long GetAttachmentTime();
    }
}
