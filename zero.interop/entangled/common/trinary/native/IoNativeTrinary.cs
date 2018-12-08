using System;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.mock;

namespace zero.interop.entangled.common.trinary.native
{
    public class IoNativeTrinary : IIoTrinary
    {
        public void GetTrits(sbyte[] flexTritsBuffer, int buffOffset, sbyte[] tritBuffer, int length)
        {
            Codec.GetTrits(flexTritsBuffer, buffOffset, tritBuffer, length);
        }

        public void GetTrytes(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int numTritsToConvert)
        {
            Codec.GetTrytes(tritBuffer, offset, tryteBuffer, numTritsToConvert);
        }

        public void GetFlexTrytes(sbyte[] tryteBuffer, int toLen, sbyte[] flexTritBuffer, int offset, int _, int numTritsToConvert)
        {
            throw new NotImplementedException();
        }
    }
}
