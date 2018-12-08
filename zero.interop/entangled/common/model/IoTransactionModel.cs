using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model
{
    [StructLayout(LayoutKind.Sequential)]
    public struct IoTransactionModel : IIoInteropTransactionModel
    {
        [IgnoreDataMember]
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_6561)]        
        public sbyte[] signature_or_message;

        [IgnoreDataMember]
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] address;        

        [IgnoreDataMember]
        public Int64 value;
                
        [IgnoreDataMember]
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] obsolete_tag;
        
        [IgnoreDataMember]
        public Int64 timestamp;
        
        [IgnoreDataMember]
        public Int64 current_index;
        
        [IgnoreDataMember]
        public Int64 last_index;

        [IgnoreDataMember]        
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] bundle;
        
        [IgnoreDataMember]        
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] trunk;
        
        [IgnoreDataMember]        
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] branch;
        
        [IgnoreDataMember]        
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] tag;
        
        [IgnoreDataMember]
        public Int64 attachment_timestamp;
        
        [IgnoreDataMember]
        public Int64 attachment_timestamp_lower;

        [IgnoreDataMember]
        public Int64 attachment_timestamp_upper;

        [IgnoreDataMember]        
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] nonce;

        [IgnoreDataMember]
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        public sbyte[] hash;

        //Metadata
        [IgnoreDataMember]
        public UInt64 snapshot_index;

        [IgnoreDataMember]
        [MarshalAs(UnmanagedType.I1)]
        public bool solid;
      
        public string Address
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_ADDRESS];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, address, 0, IoTransaction.NUM_TRITS_ADDRESS, IoTransaction.NUM_TRITS_ADDRESS);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set
            {

            }
        }

        public string Message
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_SIGNATURE];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, signature_or_message, 0, IoTransaction.NUM_TRITS_SIGNATURE, IoTransaction.NUM_TRITS_SIGNATURE);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set
            {

            }
        }

        public Int64 Value
        {
            get => value;
            set { }
        }
    

        public string ObsoleteTag
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_OBSOLETE_TAG];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, obsolete_tag, 0, IoTransaction.NUM_TRITS_OBSOLETE_TAG, IoTransaction.NUM_TRITS_OBSOLETE_TAG);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public Int64 Timestamp
        {
            get => timestamp;
            set { }
        }
        public Int64 CurrentIndex
        {
            get => current_index;
            set { }
        }
        public Int64 LastIndex
        {
            get => last_index;
            set { }
        }
        public string Bundle
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_BUNDLE];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, bundle, 0, IoTransaction.NUM_TRITS_BUNDLE, IoTransaction.NUM_TRITS_BUNDLE);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Trunk
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_TRUNK];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, trunk, 0, IoTransaction.NUM_TRITS_TRUNK, IoTransaction.NUM_TRITS_TRUNK);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Branch
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_BRANCH];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, branch, 0, IoTransaction.NUM_TRITS_BRANCH, IoTransaction.NUM_TRITS_BRANCH);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Tag
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_TAG];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, tag, 0, IoTransaction.NUM_TRITS_TAG, IoTransaction.NUM_TRITS_TAG);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public Int64 AttachmentTimestamp
        {
            get => attachment_timestamp;
            set { }
        }
        public Int64 AttachmentTimestampLower
        {
            get => attachment_timestamp_lower;
            set { }
        }
        public Int64 AttachmentTimestampUpper
        {
            get => attachment_timestamp_upper;
            set { }
        }
        public string Nonce
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_NONCE];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, nonce, 0, IoTransaction.NUM_TRITS_NONCE, IoTransaction.NUM_TRITS_NONCE);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Hash
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_HASH];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, hash, 0, IoTransaction.NUM_TRITS_HASH, IoTransaction.NUM_TRITS_HASH);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public UInt64 SnapshotIndex
        {
            get => snapshot_index;
            set { }
        }
        public bool Solid
        {
            get => solid;
            set { }
        }

        public int Pow { get; set; }

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
