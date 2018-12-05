using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace zero.interop.entangled.common.model
{
    public static class IoTransaction
    {
        public const int NUM_TRYTES_ADDRESS = 81;

        //iota_transaction_t transaction_deserialize(const flex_trit_t* trits);
        [DllImport("transaction")]
        public static extern unsafe IoTransactionModel transaction_deserialize(sbyte* trits);
    }
}
