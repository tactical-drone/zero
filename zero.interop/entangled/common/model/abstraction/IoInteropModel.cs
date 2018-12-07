using System;
using System.Runtime.InteropServices;
using System.Runtime.Loader;

namespace zero.interop.entangled.common.model.abstraction
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

        public unsafe IIoInteropTransactionModel GetTransaction(sbyte[] tritBuffer, int buffOffset = 0, sbyte[] tryteBuffer = null, int length = 0)
        {
            fixed (sbyte* trits = tritBuffer)
            {
                
                //try
                //{
                //    IoTransaction.flex_trits_slice(null, 0, null, 0, 0, 0);
                //}
                //catch (Exception e)
                //{
                //    Console.WriteLine(e);
                //}

                //var ret = IoTransaction.transaction_deserialize(trits);
                var transaction = IoTransactionModel.Init();

                IntPtr txPtr = Marshal.AllocHGlobal(Marshal.SizeOf(transaction));

                Marshal.StructureToPtr(transaction, txPtr, false);
                
      //Console.WriteLine("./libflex_trit_5.so:" + IoLib.dlopen("./libflex_trit_5.so", IoLib.RtldNow | IoLib.RtldGlobal));
                var converted = IoTransaction.transaction_deserialize_from_trits(txPtr, trits);
                Console.WriteLine($"================>{converted}<========================");

                Marshal.PtrToStructure(txPtr, transaction);
                Marshal.FreeHGlobal(txPtr);
                //return transaction;
                return transaction;
            }
        }
    }
}
