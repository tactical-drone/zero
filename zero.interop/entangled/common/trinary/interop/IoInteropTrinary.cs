using zero.interop.entangled.common.trinary.interop;

namespace zero.interop.entangled.common.trinary.abstraction
{
    /// <summary>
    /// Interop with trinary
    /// </summary>
    public class IoInteropTrinary : IIoTrinary
    {
        //static IoInteropTrinary()
        //{
        //    Console.WriteLine("./libtrit_byte.so:" + IoLib.dlopen("./libtrit_tryte.so", IoLib.RtldNow | IoLib.RtldGlobal));
        //}

        /// <summary>
        /// Get trits from flex trits
        /// </summary>
        /// <param name="flexTritsBuffer"></param>
        /// <param name="buffOffset"></param>
        /// <param name="tritBuffer"></param>
        /// <param name="length"></param>
        public unsafe void GetTritsFromFlexTrits(sbyte[] flexTritsBuffer, int buffOffset, sbyte[] tritBuffer, int length)
        {
            fixed (sbyte* bytes = &flexTritsBuffer[buffOffset])
            {
                fixed (sbyte* trits = tritBuffer)
                {
                    IoTritByte.bytes_to_trits(bytes, length, trits, length * IoTritByte.NumberOfTritsInAByte - 1);
                }
            }
        }

        /// <summary>
        /// Converts an array of trits into an array of trytes
        /// </summary>
        /// <param name="tritBuffer">The trit buffer</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="tryteBuffer">A buffer containing the result of the decoded trits</param>
        /// <param name="numTritsToConvert">The number of trits to convert</param>
        public unsafe void GetTrytesFromTrits(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int numTritsToConvert)
        {            
            fixed (sbyte* trits = &tritBuffer[offset])
            {
                fixed (sbyte* trytes = tryteBuffer)
                {
                    IoTritTryte.trits_to_trytes(trytes, trits, tritBuffer.Length);
                }
            }            
        }

        /// <summary>
        /// Get trytes from flex trit buffer
        /// </summary>
        /// <param name="tryteBuffer">The destination tryte buffer</param>
        /// <param name="toLen">The tryte buffer length</param>
        /// <param name="flexTritBuffer">The buffer containing the flex trits</param>
        /// <param name="offset">The offset into the flex trit buffer</param>
        /// <param name="numTritsAvailable">The number of trits available for conversion?</param>
        /// <param name="numTritsToConvert">The number of trits to convert</param>
        public unsafe void GetTrytesFromFlexTrits(sbyte[] tryteBuffer, int toLen, sbyte[] flexTritBuffer, int offset, int numTritsAvailable, int numTritsToConvert)
        {
            fixed (sbyte* flexTrits = &flexTritBuffer[offset])
            {
                fixed (sbyte* trytes = tryteBuffer)
                {
                    IoFlexTrit.flex_trits_to_trytes(trytes, toLen, flexTrits, numTritsAvailable, numTritsToConvert);
                }
            }
        }

        /// <summary>
        /// Get long from trits
        /// </summary>
        /// <param name="flexTritBuffer">The trit buffer</param>
        /// <param name="offset">offset into the buffer</param>
        /// <param name="numTrits">The number of trits to convert</param>
        /// <returns>The long value</returns>
        public unsafe long GetLongFromFlexTrits(sbyte[] flexTritBuffer, int offset, int numTrits)
        {            
            fixed (sbyte* flexTrits = &flexTritBuffer[offset])
            {                
                return IoTritLong.trits_to_long(flexTrits, numTrits);
            }
        }
    }
}
