using System.Linq;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.mock;
using zero.interop.utils;

namespace zero.interop.entangled.common.model.interop
{
    public class IoInteropModel : IIoInteropModel
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

        public unsafe IIoInteropTransactionModel GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {            
            fixed (sbyte* flexTrits = &flexTritBuffer[buffOffset])
            {
                IoTransaction.transaction_deserialize_from_trits(out var memMap, flexTrits);

                var interopTransaction = new IoInteropTransactionModel
                {
                    RawMapping = memMap, //TODO remove this later
                    TrimmedMap = IoMarshalledTransaction.Trim(ref memMap),
                    Pow = 0,
                };

                //Check pow
                var hashTrytes = new sbyte[IoTransaction.NUM_TRYTES_HASH];
                IoEntangled.Default.Trinary.GetFlexTrytes(hashTrytes, hashTrytes.Length, flexTritBuffer, buffOffset + Codec.TransactionSize, IoTransaction.NUM_TRITS_HASH, IoTransaction.NUM_TRITS_HASH - 9);                

                var proposedHash = Encoding.ASCII.GetString(hashTrytes.Select(c => (byte) c).ToArray());                
                var computedHash = interopTransaction.Hash;

                IIoInteropTransactionModel byref = interopTransaction;
                IoPow.Compute(ref byref, computedHash, proposedHash);                

                return byref;
            }
        }
    }
}
