using zero.interop.entangled.mock;
using zero.interop.utils;

namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// Implements a interop decoder using optimized c++ entangled decoders
    /// </summary>
    public class IoInteropModelDecoder : IIoModelDecoder<byte[]>
    {
        //static IoInteropModel()
        //{
        //    Console.WriteLine("./libtrit.so:" + IoLib.dlopen("./libtrit.so", IoLib.RtldNow | IoLib.RtldGlobal));

        //    Console.WriteLine("./libtrit_tryte.so:" + IoLib.dlopen("./libtrit_tryte.so", IoLib.RtldNow | IoLib.RtldGlobal));

        //   Console.WriteLine("./libdigest.so:" + IoLib.dlopen("./libdigest.so", IoLib.RtldNow | IoLib.RtldGlobal));           

        //   Console.WriteLine("./libtrit_long.so:" + IoLib.dlopen("./libtrit_long.so", IoLib.RtldNow | IoLib.RtldGlobal));

        //   Console.WriteLine("./libflex_trit_5.so:" + IoLib.dlopen("./libflex_trit_5.so", IoLib.RtldNow | IoLib.RtldGlobal));

        //    Console.WriteLine("./libflex_trit_array.so:" + IoLib.dlopen("./libflex_trit_array.so", IoLib.RtldNow | IoLib.RtldGlobal));

        //    Console.WriteLine("./libtransaction.so:" + IoLib.dlopen("./libtransaction.so", IoLib.RtldNow | IoLib.RtldGlobal));

        //    Console.WriteLine("./libinterop.so:" + IoLib.dlopen("./libinterop.so", IoLib.RtldNow | IoLib.RtldGlobal));
        //}

        /// <summary>
        /// Deserialize ioterop model from flex trits
        /// </summary>
        /// <param name="flexTritBuffer">The flex trits</param>
        /// <param name="buffOffset">Offset into the buffer</param>
        /// <param name="tritBuffer">Some buffer space</param>
        /// <returns>The deserialized transaction</returns>
        public unsafe IIoTransactionModel<byte[]> GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {            
            fixed (sbyte* flexTrits = &flexTritBuffer[buffOffset])
            {                
                IoTransaction.transaction_deserialize_from_trits(out var memMap, flexTrits);

                var interopTransaction = new IoInteropTransactionModel
                {                    
                    SignatureOrMessage = IoMarshalledTransaction.Trim(memMap.signature_or_message),
                    Address = IoMarshalledTransaction.Trim(memMap.address, 0),
                    Value = memMap.value,
                    ObsoleteTag = IoMarshalledTransaction.Trim(memMap.obsolete_tag, 1),
                    Timestamp = memMap.timestamp,
                    CurrentIndex = memMap.current_index,
                    LastIndex = memMap.last_index,
                    Bundle = memMap.bundle,
                    Trunk = IoMarshalledTransaction.Trim(memMap.trunk),
                    Branch = IoMarshalledTransaction.Trim(memMap.branch),
                    Tag = IoMarshalledTransaction.Trim(memMap.tag, 0),
                    AttachmentTimestamp = memMap.attachment_timestamp,
                    AttachmentTimestampLower = memMap.attachment_timestamp_lower,
                    AttachmentTimestampUpper = memMap.attachment_timestamp_upper,
                    Nonce = IoMarshalledTransaction.Trim(memMap.nonce),                    
                };

                //Check pow
                IoPow.ComputeFromBytes(interopTransaction, memMap.hash, flexTritBuffer, buffOffset + Codec.TransactionSize);

                interopTransaction.Hash = IoMarshalledTransaction.Trim(memMap.hash);

                return interopTransaction;
            }
        }
    }
}
