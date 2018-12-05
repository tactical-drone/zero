using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.common.trinary.abstraction;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model
{   
    public unsafe struct IoTransactionModel :IIoInteropTransactionModel
    {
        // 2187 trytes = 6561 trits
        [IgnoreDataMember]
        public fixed sbyte signature_or_message[IoFlexTrit.FLEX_TRIT_SIZE_6561];
        // 81 trytes = 243 trits
        [IgnoreDataMember]
        public fixed sbyte address[IoFlexTrit.FLEX_TRIT_SIZE_243];
        // 27 trytes = 81 trits
        [IgnoreDataMember]
        public Int64 value;
        // 27 trytes = 81 trits
        [IgnoreDataMember]
        public fixed sbyte obsolete_tag[IoFlexTrit.FLEX_TRIT_SIZE_81];
        // 9 trytes = 27 trits
        [IgnoreDataMember]
        public Int64 timestamp;
        // 9 trytes = 27 trits
        [IgnoreDataMember]
        public Int64 current_index;
        // 9 trytes = 27 trits
        [IgnoreDataMember]
        public Int64 last_index;
        // 81 trytes = 243 trits
        [IgnoreDataMember]
        public fixed sbyte bundle[IoFlexTrit.FLEX_TRIT_SIZE_243];
        // 81 trytes = 243 trits
        [IgnoreDataMember]
        public fixed sbyte trunk[IoFlexTrit.FLEX_TRIT_SIZE_243];
        // 81 trytes = 243 trits
        [IgnoreDataMember]
        public fixed sbyte branch[IoFlexTrit.FLEX_TRIT_SIZE_243];
        // 27 trytes = 81 trits
        [IgnoreDataMember]
        public fixed sbyte tag[IoFlexTrit.FLEX_TRIT_SIZE_81];
        // 9 trytes = 27 trits
        [IgnoreDataMember]
        public Int64 attachment_timestamp;
        // 9 trytes = 27 trits
        [IgnoreDataMember]
        public Int64 attachment_timestamp_lower;
        // 9 trytes = 27 trits
        [IgnoreDataMember]
        public Int64 attachment_timestamp_upper;
        // 27 trytes = 81 trits
        [IgnoreDataMember]
        public fixed sbyte nonce[IoFlexTrit.FLEX_TRIT_SIZE_81];
        // 81 trytes = 243 trits
        [IgnoreDataMember]
        public fixed sbyte hash[IoFlexTrit.FLEX_TRIT_SIZE_243];
        // Total 2754 trytes

        //Metadata
        [IgnoreDataMember]
        public UInt64 snapshot_index;
        [IgnoreDataMember]
        public bool solid;

        public string Address { get; set; }
        public string Message { get; set; }
        public long Value { get; set; }
        public string ObsoleteTag { get; set; }
        public long Timestamp { get; set; }
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
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
    }
}
