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
        public byte[] address;

        public long value;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]

        public byte[] obsolete_tag;

        public long timestamp;
        [Column(nameof(current_index)), ClusteringKey]
        public long current_index;
        public long last_index;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]
        [Column(nameof(bundle)), PartitionKey]    
        [Frozen]
        public byte[] bundle;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] trunk;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] branch;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]        
        public byte[] tag;

        public long attachment_timestamp;
        public long attachment_timestamp_lower;
        public long attachment_timestamp_upper;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_81)]        
        public byte[] nonce;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
        public byte[] hash;
        //Metadata
        [Ignore]
        public long snapshot_index;

        [MarshalAs(UnmanagedType.I1)]
        [Ignore]
        public bool solid;

    }
}
