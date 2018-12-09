using System;
using System.Linq;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.mock;

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
                var txStruct = new IoTransactionModel
                {
                    Pow = 0
                };                

                IoTransaction.transaction_deserialize_from_trits(out txStruct, flexTrits);

                //Check pow
                var hashTrytes = new sbyte[IoTransaction.NUM_TRYTES_HASH];
                IoEntangled.Default.Trinary.GetFlexTrytes(hashTrytes, hashTrytes.Length, flexTritBuffer, buffOffset + Codec.TransactionSize, IoTransaction.NUM_TRITS_HASH, IoTransaction.NUM_TRITS_HASH - 9);                

                var proposedHash = Encoding.ASCII.GetString(hashTrytes.Select(c => (byte) c).ToArray());                
                var computedHash = txStruct.Hash;

                if (computedHash.Contains(proposedHash.Substring(0, 10)) &&
                    computedHash.Substring(computedHash.Length - 11, 6)
                        .Equals(proposedHash.Substring(proposedHash.Length - 11, 6)))
                {
                    for (var i = computedHash.Length - 1; i > 0 && computedHash[i--] == '9'; txStruct.Pow++) { }
                    txStruct.FakePow = 0;
                }
                else
                {
                    for (var i = computedHash.Length - 1; i > 0 && computedHash[i--] == '9'; txStruct.Pow--) { }
                    txStruct.FakePow = txStruct.Pow;
                    for (var i = proposedHash.Length - 1; i > 0 && proposedHash[i--] == '9'; txStruct.FakePow++) { }

                    if (txStruct.Pow == -IoTransaction.NUM_TRYTES_HASH)
                    {
                        txStruct.Pow = 0;
                        txStruct.FakePow = -(txStruct.FakePow + IoTransaction.NUM_TRYTES_HASH);
                        if (txStruct.FakePow == IoTransaction.NUM_TRYTES_HASH)
                            txStruct.FakePow = 0;
                    }
                }                

                return txStruct;
            }
        }
    }
}
