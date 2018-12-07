using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model
{
    [StructLayout(LayoutKind.Sequential)]
    public class IoTransactionModel : IIoInteropTransactionModel
    {
        // 2187 trytes = 6561 trits
        //[IgnoreDataMember]
        //public fixed sbyte signature_or_message[IoFlexTrit.FLEX_TRIT_SIZE_6561];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_6561)]
        public sbyte[] signature_or_message;
        // 81 trytes = 243 trits
        //[IgnoreDataMember]
        //public fixed sbyte address[IoFlexTrit.FLEX_TRIT_SIZE_243];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] address;
        // 27 trytes = 81 trits
        //[IgnoreDataMember]
        public Int64 value;        
        // 27 trytes = 81 trits
        //[IgnoreDataMember]
        //public fixed sbyte obsolete_tag[IoFlexTrit.FLEX_TRIT_SIZE_81];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] obsolete_tag;
        // 9 trytes = 27 trits
        //[IgnoreDataMember]
        public Int64 timestamp;
        // 9 trytes = 27 trits
        //[IgnoreDataMember]
        public Int64 current_index;
        // 9 trytes = 27 trits
        //[IgnoreDataMember]
        public Int64 last_index;
        // 81 trytes = 243 trits
        //[IgnoreDataMember]
        //public fixed sbyte bundle[IoFlexTrit.FLEX_TRIT_SIZE_243];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] bundle;
        // 81 trytes = 243 trits
        //[IgnoreDataMember]
        //public fixed sbyte trunk[IoFlexTrit.FLEX_TRIT_SIZE_243];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] trunk;
        // 81 trytes = 243 trits
        //[IgnoreDataMember]
        //public fixed sbyte branch[IoFlexTrit.FLEX_TRIT_SIZE_243];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] branch;
        // 27 trytes = 81 trits
        //[IgnoreDataMember]
        //public fixed sbyte tag[IoFlexTrit.FLEX_TRIT_SIZE_81];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] tag;
        // 9 trytes = 27 trits
        //[IgnoreDataMember]
        public Int64 attachment_timestamp;
        // 9 trytes = 27 trits
        //[IgnoreDataMember]
        public Int64 attachment_timestamp_lower;
        // 9 trytes = 27 trits
        //[IgnoreDataMember]
        public Int64 attachment_timestamp_upper;
        // 27 trytes = 81 trits
        //[IgnoreDataMember]
        //public fixed sbyte nonce[IoFlexTrit.FLEX_TRIT_SIZE_81];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] nonce;
        // 81 trytes = 243 trits
        //[IgnoreDataMember]
        //public fixed sbyte hash[IoFlexTrit.FLEX_TRIT_SIZE_243];
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] hash;
        // Total 2754 trytes

        //Metadata
        //[IgnoreDataMember]
        public UInt64 snapshot_index;

        //[IgnoreDataMember]
        [MarshalAs(UnmanagedType.I1)]
        public bool solid;

        public static IoTransactionModel Init()
        {
            var tx = new IoTransactionModel
            {
                signature_or_message = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_6561 * 2],
                address = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_243 * 2],
                obsolete_tag = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_81 * 2],
                bundle = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_243 * 2],
                trunk = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_243 * 2],
                branch = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_243 * 2],
                tag = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_81 * 2],
                nonce = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_81 * 2],
                hash = new sbyte[IoFlexTrit.FLEX_TRIT_SIZE_243 * 2]
            };
            return tx;
        }        

        public string Address
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_ADDRESS];
                IoEntangled.Default.Trinary.GetTrytes(address,0, trytes, IoTransaction.NUM_TRITS_ADDRESS);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set
            {

            }
        }

        public string Message { get; set; }

        public Int64 Value
        {
            get => value;
            set { }
        }
    

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
        public Int64 SnapshotIndex { get; set; }
        public bool Solid { get; set; }
    }    
}
