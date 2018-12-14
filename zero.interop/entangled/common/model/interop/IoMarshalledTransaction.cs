using System.Runtime.InteropServices;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.trinary;

namespace zero.interop.entangled.common.model.interop
{

    [StructLayout(LayoutKind.Sequential)]
    public struct IoMarshalledTransaction
    {
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_6561)]        
        public byte[] signature_or_message;

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
        public byte[] tag;

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

    }
}
