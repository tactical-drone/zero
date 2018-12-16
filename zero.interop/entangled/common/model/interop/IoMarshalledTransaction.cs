using System;
using System.Runtime.InteropServices;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;

namespace zero.interop.entangled.common.model.interop
{

    [StructLayout(LayoutKind.Sequential)]
    public struct IoMarshalledTransaction
    {
        public static IoMarshalledTransaction Trim(ref IoMarshalledTransaction transaction)
        {
            return new IoMarshalledTransaction
            {
                signature_or_message = IoMarshalledTransaction.Trim(transaction.signature_or_message),
                address = IoMarshalledTransaction.Trim(transaction.address, new sbyte[] { }),
                value = transaction.value,
                obsolete_tag = IoMarshalledTransaction.Trim(transaction.obsolete_tag),
                timestamp = transaction.timestamp,
                current_index = transaction.current_index,
                last_index = transaction.last_index,
                bundle = transaction.bundle,
                trunk = transaction.trunk,
                branch = transaction.branch,
                tag = IoMarshalledTransaction.Trim(transaction.tag, new sbyte[] { }),
                attachment_timestamp = transaction.attachment_timestamp,
                attachment_timestamp_lower = transaction.attachment_timestamp_lower,
                attachment_timestamp_upper = transaction.attachment_timestamp_upper,
                nonce = transaction.nonce,
                hash = transaction.hash
            };
        }

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_6561)]                
        public sbyte[] signature_or_message;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]                
        public sbyte[] address;
        
        public long value;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public sbyte[] obsolete_tag;
        
        public long timestamp;
                
        public long current_index;
        
        public long last_index;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] bundle;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] trunk;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] branch;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]        
        public sbyte[] tag;
        
        public long attachment_timestamp;
        
        public long attachment_timestamp_lower;
        
        public long attachment_timestamp_upper;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]        
        public byte[] nonce;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        
        public byte[] hash;
        //Metadata
        //[PartitionKey(1)]
        
        public long snapshot_index;

        [MarshalAs(UnmanagedType.I1)]        
        public bool solid;

        public static sbyte[] Trim(sbyte[] buffer, sbyte[] emptySet = null)
        {
            
            for (var i = buffer.Length ; i--> 0;)
            {
                if (buffer[i] != 0)
                {                   
                    return i == buffer.Length - 1 ? buffer : buffer.AsSpan().Slice(0, buffer.Length - i - 1).ToArray();
                }
            }

            return emptySet;
        }


        public short Size
        {
            get => (short) (Codec.TransactionSize - (IoFlexTrit.FLEX_TRIT_SIZE_81 - tag.Length) - (IoFlexTrit.FLEX_TRIT_SIZE_6561 - signature_or_message?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_6561) );
            set { }
        }
    }
}
