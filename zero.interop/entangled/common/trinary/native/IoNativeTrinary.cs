using System;
using System.Text;
using Tangle.Net.Cryptography;
using Tangle.Net.Entity;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.native;

namespace zero.interop.entangled.common.trinary.native
{
    /// <summary>
    /// Mock interop trinary
    /// </summary>
    public class IoNativeTrinary : IIoTrinary
    {
        public void GetTritsFromFlexTrits(sbyte[] flexTritsBuffer, int buffOffset, sbyte[] tritBuffer, int length)
        {
            Codec.GetTrits(flexTritsBuffer, buffOffset, tritBuffer, length);
        }

        public void GetTrytesFromTrits(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int numTritsToConvert)
        {
            Codec.GetTrytes(tritBuffer, offset, tryteBuffer, numTritsToConvert);
        }

        public void GetTrytesFromFlexTrits(sbyte[] tryteBuffer, int toLen, sbyte[] flexTritBuffer, int offset, int _, int numTritsToConvert)
        {
            throw new NotImplementedException();
        }

        public long GetLongFromFlexTrits(sbyte[] tryteBuffer, int offset, int numTrits)
        {
            return Converter.ConvertTritsToBigInt(new TryteString(Encoding.UTF8.GetString((byte[])(Array)tryteBuffer)).ToTrits(), offset, numTrits).LongValue;
        }
    }
}
