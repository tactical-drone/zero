using System;
using System.Collections.Generic;
using System.Text;

namespace zero.interop.entangled.common.trinary.abstraction
{
    public class IoInteropTrinary : IIoTrinary
    {
        public unsafe void GetTrits(sbyte[] buffer, int buffOffset, sbyte[] tritBuffer, int length)
        {
            fixed (sbyte* bytes = &buffer[buffOffset])
            {
                fixed (sbyte* trits = tritBuffer)
                {
                    IoTritByte.bytes_to_trits(bytes, length, trits, length * IoTritByte.NumberOfTritsInAByte - 1);
                }
            }
        }

        public unsafe void GetTrytes(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int length)
        {            
            fixed (sbyte* trits = &tritBuffer[offset])
            {
                fixed (sbyte* trytes = tryteBuffer)
                {
                    IoTritTryte.trits_to_trytes(trytes, trits, tritBuffer.Length);
                }
            }            
        }
    }
}
