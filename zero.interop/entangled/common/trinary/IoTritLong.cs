using System.Runtime.InteropServices;

namespace zero.interop.entangled.common.trinary
{
    public static class IoTritLong
    {
        //size_t min_bytes(size_t num_trits);
        [DllImport("interop")]
        public static extern unsafe long trits_to_long(sbyte* trits, int numTrits);
    }
}
