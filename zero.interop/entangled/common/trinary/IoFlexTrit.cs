using System.Runtime.InteropServices;

namespace zero.interop.entangled.common.trinary
{
    public static class IoFlexTrit
    {        
#if FLEX_TRIT_ENCODING_1_TRIT_PER_BYTE
        public const int FLEX_TRIT_SIZE_27 = 27;
        public const int FLEX_TRIT_SIZE_81 = 81;
        public const int FLEX_TRIT_SIZE_243 = 243;
        public const int FLEX_TRIT_SIZE_6561 = 6561;
        public const int FLEX_TRIT_SIZE_8019 = 8019;
        public const int NUM_TRITS_PER_FLEX_TRIT = 1;
#elif FLEX_TRIT_ENCODING_3_TRITS_PER_BYTE
        public const int FLEX_TRIT_SIZE_27 = 9;
        public const int FLEX_TRIT_SIZE_81 = 27;
        public const int FLEX_TRIT_SIZE_243 = 81;
        public const int FLEX_TRIT_SIZE_6561 = 2187;
        public const int FLEX_TRIT_SIZE_8019 = 2673;
        public const int NUM_TRITS_PER_FLEX_TRIT = 3
#elif FLEX_TRIT_ENCODING_4_TRITS_PER_BYTE
        public const int FLEX_TRIT_SIZE_27 = 7;
        public const int FLEX_TRIT_SIZE_81 = 21;
        public const int FLEX_TRIT_SIZE_243 = 61;
        public const int FLEX_TRIT_SIZE_6561 = 1641;
        public const int FLEX_TRIT_SIZE_8019 = 2005;
        public const int NUM_TRITS_PER_FLEX_TRIT 4
#elif FLEX_TRIT_ENCODING_5_TRITS_PER_BYTE
        public const int FLEX_TRIT_SIZE_27 = 6;
        public const int FLEX_TRIT_SIZE_81 = 17;
        public const int FLEX_TRIT_SIZE_243 = 49;
        public const int FLEX_TRIT_SIZE_6561 = 1313;
        public const int FLEX_TRIT_SIZE_8019 = 1604;
        public const int NUM_TRITS_PER_FLEX_TRIT = 5;
#endif

        //size_t flex_trits_from_trits(flex_trit_t*const to_flex_trits, size_t const to_len, trit_t const *const trits, size_t const len, size_t const num_trits);
        [DllImport("interop")]
        public static extern unsafe int flex_trits_from_trits(sbyte *toFlexTrits, int toLen, sbyte *trits, int len, int numTrits);

        [DllImport("interop")]
        public static extern unsafe int flex_trits_to_trytes(sbyte* trytes, int toLen, sbyte* flexTrits, int _, int numTrits);


    }
}
