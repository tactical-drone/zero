using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model
{
    public static unsafe class IoTransaction
    {
        public const int NUM_TRYTES_ADDRESS = 81;
        
        [DllImport("interop")]
        public static extern int transaction_deserialize_from_trits(IoTransactionModel transaction, sbyte* trits);
    }
}
