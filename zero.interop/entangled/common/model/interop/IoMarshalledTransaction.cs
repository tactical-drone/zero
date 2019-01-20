using System;
using System.Runtime.InteropServices;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;

namespace zero.interop.entangled.common.model.interop
{

    /// <summary>
    /// Transaction model used to marshal data between C# and C++
    /// </summary>
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
                
        public long current_index;
        
        public long last_index;

        [MarshalAs(UnmanagedType.ByValArray, SizeConst = IoFlexTrit.FLEX_TRIT_SIZE_243)]        
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
        //[PartitionKey(1)]
        
        public long snapshot_index;

        [MarshalAs(UnmanagedType.I1)]        
        public bool solid;

        public static byte[] Trim(byte[] buffer, byte nullSet = 9)
        {            
            for (var i = buffer.Length; i-- > 0;)
            {
                if (buffer[i] != 0)
                {
                    var trimmed = i == buffer.Length - 1 ? buffer : buffer.AsSpan().Slice(0, i + 1).ToArray(); //TODO ?

                    if (trimmed.Length != 0) return trimmed;
                    if (nullSet == 9)
                        return null;

                    return nullSet > 0 ? new[] { nullSet } : new byte[]{};
                }
            }

            if (nullSet == 9)
                return null;
            return nullSet > 0 ? new[] { nullSet } : new byte[] { };
        }

        public short Size
        {
            get => (short) (Codec.TransactionSize                             
                            - (IoFlexTrit.FLEX_TRIT_SIZE_6561 - signature_or_message?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_6561) 
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - address?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_81 - obsolete_tag?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_81)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - trunk?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - branch?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_81 - tag.Length)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_81 - nonce?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_81)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - hash?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243));

            set { }
        }
    }
}
