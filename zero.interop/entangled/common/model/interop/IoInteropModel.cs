using System.Linq;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;
using zero.interop.utils;

namespace zero.interop.entangled.common.model.interop
{
    public class IoInteropModel : IIoInteropModel<byte[]>
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

        public unsafe IIoInteropTransactionModel<byte[]> GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {            
            fixed (sbyte* flexTrits = &flexTritBuffer[buffOffset])
            {                
                IoTransaction.transaction_deserialize_from_trits(out var memMap, flexTrits);

                var interopTransaction = new IoInteropTransactionModel
                {                    
                    SignatureOrMessage = IoMarshalledTransaction.Trim(memMap.signature_or_message),
                    Address = IoMarshalledTransaction.Trim(memMap.address, new byte[] { }),
                    Value = memMap.value,
                    ObsoleteTag = IoMarshalledTransaction.Trim(memMap.obsolete_tag),
                    Timestamp = memMap.timestamp,
                    CurrentIndex = memMap.current_index,
                    LastIndex = memMap.last_index,
                    Bundle = memMap.bundle,
                    Trunk = IoMarshalledTransaction.Trim(memMap.trunk),
                    Branch = IoMarshalledTransaction.Trim(memMap.branch),
                    Tag = IoMarshalledTransaction.Trim(memMap.tag, new byte[] { }),
                    AttachmentTimestamp = memMap.attachment_timestamp,
                    AttachmentTimestampLower = memMap.attachment_timestamp_lower,
                    AttachmentTimestampUpper = memMap.attachment_timestamp_upper,
                    Nonce = IoMarshalledTransaction.Trim(memMap.nonce),
                    Hash = IoMarshalledTransaction.Trim(memMap.hash),                    
                    Pow = 0,
                };

                //Check pow
                var hashTrytes = new sbyte[IoTransaction.NUM_TRYTES_HASH];
                IoEntangled<byte[]>.Default.Ternary.GetFlexTrytes(hashTrytes, hashTrytes.Length, flexTritBuffer, buffOffset + Codec.TransactionSize, IoTransaction.NUM_TRITS_HASH, IoTransaction.NUM_TRITS_HASH - 9);                

                var proposedHash = Encoding.ASCII.GetString(hashTrytes.Select(c => (byte) c).ToArray());                
                var computedHash = interopTransaction.Hash;

                IIoInteropTransactionModel<byte[]> byref = interopTransaction;
                IoPow.Compute(ref byref, byref.AsTrytes(computedHash, IoTransaction.NUM_TRITS_HASH) , proposedHash);                

                return byref;
            }
        }
    }
}
