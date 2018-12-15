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
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_6561)]                
        public sbyte[] signature_or_message;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]                
        public byte[] address;
        
        public long value;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public byte[] obsolete_tag;
        
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
        
        public sbyte[] Body
        {
            get
            {
                for (var i = signature_or_message.Length - 1; i >= 0; i--)
                {                    
                    if (signature_or_message[i] != 0)
                    {
                        return i == signature_or_message.Length - 1 ? signature_or_message : signature_or_message.AsSpan().Slice(0, signature_or_message.Length - i).ToArray();
                    }                    
                }

                return null;
            }
            set { }
        }
        
        public sbyte[] Tag
        {
            get
            {                
                for (var i = tag.Length - 1; i >= 0; i--)
                {
                    if (tag[i] != 0)
                    {                            
                        return i == tag.Length - 1 ? tag : tag.AsSpan().Slice(0, tag.Length - i).ToArray();
                    }
                }

                return new sbyte[]{};
            }
            set { }
        }

        public short Size
        {
            get => (short) (Codec.TransactionSize - (tag.Length - Tag?.Length??0) - (signature_or_message.Length - Body?.Length??0));
            set { }
        }
    }
}
