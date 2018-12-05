using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.mock;

namespace zero.interop.entangled.common.trinary.native
{
    public class IoNativeTrinary : IIoTrinary
    {
        public void GetTrits(sbyte[] buffer, int buffOffset, sbyte[] tritBuffer, int length)
        {
            Codec.GetTrits(buffer, buffOffset, tritBuffer, length);
        }

        public void GetTrytes(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int length)
        {
            Codec.GetTrytes(tritBuffer, offset, tryteBuffer, length);
        }
    }
}
