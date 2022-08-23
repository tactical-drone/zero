using System;
using System.Runtime.InteropServices;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.native;

namespace zero.interop.entangled.common.model.interop.transaction
{

    /// <summary>
    /// Transaction model used to marshal data between C# and C++
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct IoMarshalledTransaction
    {        
        public IoTxEssence essence;
        public IoTxAttachments attachments;
        public IoTxConsensus consensus;
        public IoTxData data;
        public IoTxMetadata metadata;
        public IoTxMask loaded_columns_mask;

        public static ReadOnlyMemory<byte> Trim(byte[] buffer, byte nullSet = 9)
        {            
            for (var i = buffer.Length; i-- > 0;)
            {
                if (buffer[i] != 0)
                {
                    var trimmed = i == buffer.Length - 1 ? buffer : buffer.AsMemory().Slice(0, i + 1);

                    if (trimmed.Length != 0)
                    {                      
                        //Console.WriteLine($"{BitConverter.ToString(buffer).Replace("-", "").ToLower()} ====> {BitConverter.ToString(trimmed.ToArray()).Replace("-", "").ToLower()} ");
                        return trimmed.ToArray(); //We have to copy or because we cannot go back to a pure byte[] with a correct length value
                    }
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
                            - (IoFlexTrit.FLEX_TRIT_SIZE_6561 - data.signature_or_message?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_6561) 
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - essence.address?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_81 - essence.obsolete_tag?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_81)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - attachments.trunk?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - attachments.branch?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_81 - attachments.tag.Length)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_81 - attachments.nonce?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_81)
                            - (IoFlexTrit.FLEX_TRIT_SIZE_243 - consensus.hash?.Length?? IoFlexTrit.FLEX_TRIT_SIZE_243));

            set { }
        }
    }
}
