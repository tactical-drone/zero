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
        [Ignore]
        public sbyte[] signature_or_message;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        [ClusteringKey(5)]
        public byte[] address;

        [ClusteringKey(2)]
        public long value;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        public byte[] obsolete_tag;

        [ClusteringKey(9)]
        public long timestamp;
        
        [ClusteringKey(1)]
        public long current_index;

        [ClusteringKey(0)]
        public long last_index;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        [Column(nameof(bundle)), PartitionKey(0)]    
        [Frozen]
        public byte[] bundle;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] trunk;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] branch;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]
        [ClusteringKey(3)]
        public sbyte[] tag;

        [ClusteringKey(6)]
        public long attachment_timestamp;
        [ClusteringKey(7)]
        public long attachment_timestamp_lower;
        [ClusteringKey(8)]
        public long attachment_timestamp_upper;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]        
        public byte[] nonce;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        [ClusteringKey(4)]
        public byte[] hash;
        //Metadata
        //[PartitionKey(1)]
        [Ignore]
        public long snapshot_index;

        [MarshalAs(UnmanagedType.I1)]
        [Ignore]
        public bool solid;
        
        public sbyte[] Body
        {
            get
            {
                for (var i = signature_or_message.Length - 1; i != 0; i--)
                {                    
                    if (signature_or_message[i] != 0)
                    {
                        return i == signature_or_message.Length - 1 ? signature_or_message : signature_or_message.AsSpan().Slice(0, i).ToArray();
                    }                    
                }

                return null;
            }
            set { }
        }

        [Frozen]
        public sbyte[] Tag
        {
            get
            {
                for (var i = tag.Length - 1; i != 0; i--)
                {
                    if (tag[i] != 0)
                    {
                        return i == tag.Length - 1 ? tag : tag.AsSpan().Slice(0, i).ToArray();
                    }
                }

                return new[] {(sbyte)9};
            }
            set { }
        }

        public short Size
        {
            get => (short)(Codec.TransactionSize - (tag.Length - Tag.Length) - (signature_or_message.Length - Body.Length));
            set { }
        }
    }
}
