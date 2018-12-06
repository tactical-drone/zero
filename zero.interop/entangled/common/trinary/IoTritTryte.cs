using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace zero.interop.entangled.common.trinary
{
    public static class IoTritTryte
    {
        //size_t num_trytes_for_trits(size_t num_trits);
        [DllImport("interop")]
        public static extern int num_trytes_for_trits(int num_trits);

        //trit_t get_trit_at(tryte_t*const trytes, size_t const length, size_t index);
        [DllImport("interop")]
        public static extern sbyte get_trit_at(sbyte trytes, int length, int index);

        //uint8_t set_trit_at(tryte_t*const trytes, size_t const length, size_t index,trit_t trit);
        [DllImport("interop")]
        public static extern byte set_trit_at(sbyte trytes, int length, int index,sbyte trit);

        //void trits_to_trytes(trit_t const *const trits, tryte_t *const trytes,size_t const length);
        [DllImport("interop")]
        public static extern unsafe void trits_to_trytes(sbyte *trits, sbyte *trytes, int length);


        //void trytes_to_trits(tryte_t const *const tryte, trit_t *const trits,size_t const length);
        [DllImport("interop")]
        public static extern unsafe void trytes_to_trits(sbyte *tryte, sbyte *trits,int length);
    }
}
