using System;
using zero.core.misc;
using zero.core.models;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.mock;
using zero.interop.utils;

namespace zero.tangle.entangled.common.model
{
    /// <summary>
    /// Implements a interop decoder using optimized c++ entangled decoders
    /// </summary>
    public class EntangledDecoder : IIoModelDecoder<byte[]>
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
        public  IIoTransactionModelInterface GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {
            return IoMarshalTransactionDecoder.Deserialize(flexTritBuffer, buffOffset, memMap =>
            {
                var interopTransaction = new EntangledTransaction
                {
                    SignatureOrMessageBuffer = IoMarshalledTransaction.Trim(memMap.signature_or_message),
                    AddressBuffer = IoMarshalledTransaction.Trim(memMap.address, 0),
                    Value = memMap.value,
                    ObsoleteTagBuffer = memMap.obsolete_tag,//IoMarshalledTransaction.Trim(memMap.obsolete_tag, 1),
                    Timestamp = memMap.timestamp.NormalizeDateTime(),
                    CurrentIndex = memMap.current_index,
                    LastIndex = memMap.last_index,
                    BundleBuffer = memMap.bundle,
                    TrunkBuffer = IoMarshalledTransaction.Trim(memMap.trunk),
                    BranchBuffer = IoMarshalledTransaction.Trim(memMap.branch),
                    TagBuffer = IoMarshalledTransaction.Trim(memMap.tag, 0),
                    AttachmentTimestamp = memMap.attachment_timestamp.NormalizeDateTime(),
                    AttachmentTimestampLower = memMap.attachment_timestamp_lower.NormalizeDateTime(),
                    AttachmentTimestampUpper = memMap.attachment_timestamp_upper.NormalizeDateTime(),
                    NonceBuffer = IoMarshalledTransaction.Trim(memMap.nonce),                    
                    Blob = new ReadOnlyMemory<byte>((byte[])(Array)flexTritBuffer).Slice(buffOffset, Codec.MessageSize) //TODO double check                                       
                };

                interopTransaction.PopulateTotalSize();

                //Check pow
                Pow<byte[]>.ComputeFromBytes(interopTransaction, memMap.hash, flexTritBuffer, buffOffset + Codec.TransactionSize);

                interopTransaction.HashBuffer = IoMarshalledTransaction.Trim(memMap.hash);

                return interopTransaction;
            });                                
        }
    }
}
