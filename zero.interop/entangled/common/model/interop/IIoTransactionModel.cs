﻿using System;
using System.Runtime.Serialization;

// ReSharper disable InconsistentNaming
namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// The transaction model contract used throughout the system
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public interface IIoTransactionModel<TBlob>    
    {
        [DataMember]
        TBlob SignatureOrMessage { get; set; }

        [DataMember]
        TBlob Address { get; set; }

        [DataMember]
        long Value { get; set; }

        [DataMember]
        TBlob ObsoleteTag { get; set; }

        [DataMember]
        long Timestamp { get; set; }

        [DataMember]
        long CurrentIndex { get; set; }

        [DataMember]
        long LastIndex { get; set; }

        [DataMember]
        TBlob Bundle { get; set; }

        [DataMember]
        TBlob Trunk { get; set; }

        [DataMember]
        TBlob Branch { get; set; }

        [DataMember]
        TBlob Tag { get; set; }

        [DataMember]
        long AttachmentTimestamp { get; set; }

        [DataMember]
        long AttachmentTimestampLower { get; set; }

        [DataMember]
        long AttachmentTimestampUpper { get; set; }

        [DataMember]
        TBlob Nonce { get; set; }

        [DataMember]
        TBlob Hash { get; set; }

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
        
        string AsTrytes(TBlob field, int fixedLenTritsToConvert = 0);
    }
}