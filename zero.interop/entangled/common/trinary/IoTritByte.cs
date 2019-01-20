using System.Runtime.InteropServices;

namespace zero.interop.entangled.common.trinary
{
    /// <summary>
    /// Interop with trit byte
    /// </summary>
    public static class IoTritByte
    {
        public const int NumberOfTritsInAByte = 5;

        //size_t min_bytes(size_t num_trits);
        [DllImport("interop")]
        public static extern int min_bytes(int numTrits);
        
        //byte_t trits_to_byte(trit_t const *const trits, byte_t const cum,size_t const num_trits);
        [DllImport("interop")]
        public static extern unsafe sbyte trits_to_byte(sbyte* trits, sbyte cum, int numTrits);

        //void trits_to_bytes(trit_t* trits, byte_t* bytes, size_t num_trits);
        [DllImport("interop")]
        public static extern unsafe void trits_to_bytes(sbyte* trits, byte* bytes, int numTrits);


        //void byte_to_trits(byte_t byte, trit_t *const trit, size_t const num_trits);
        [DllImport("interop")]
        public static extern unsafe void byte_to_trits(sbyte oneByte, sbyte* trits, int numTrits);


        //void bytes_to_trits(byte_t const *const bytes, size_t const num_bytes, trit_t* const trits, size_t const num_trits);
        [DllImport("interop")]
        public static extern unsafe void bytes_to_trits(sbyte* bytes, int numBytes, sbyte* trits, int numTrits);
    }
}