namespace zero.interop.entangled.common.trinary.interop
{
    /// <summary>
    /// Interop with Trinary
    /// </summary>
    public interface IIoTrinary
    {
        /// <summary>
        /// Get trits from flex trits
        /// </summary>
        /// <param name="flexTritsBuffer"></param>
        /// <param name="buffOffset"></param>
        /// <param name="tritBuffer"></param>
        /// <param name="length"></param>
        void GetTritsFromFlexTrits(sbyte[] flexTritsBuffer, int buffOffset, sbyte[] tritBuffer, int length);

        /// <summary>
        /// Converts an array of trits into an array of trytes
        /// </summary>
        /// <param name="tritBuffer">The trit buffer</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="tryteBuffer">A buffer containing the result of the decoded trits</param>
        /// <param name="numTritsToConvert">The number of trits to convert</param>
        void GetTrytesFromTrits(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int numTritsToConvert);

        /// <summary>
        /// Get trytes from flex trit buffer
        /// </summary>
        /// <param name="tryteBuffer">The destination tryte buffer</param>
        /// <param name="toLen">The tryte buffer length</param>
        /// <param name="flexTritBuffer">The buffer containing the flex trits</param>
        /// <param name="offset">The offset into the flex trit buffer</param>
        /// <param name="numTritsAvailable">The number of trits available for conversion?</param>
        /// <param name="numTritsToConvert">The number of trits to convert</param>
        void GetTrytesFromFlexTrits(sbyte[] tryteBuffer, int toLen, sbyte[] flexTritBuffer, int offset, int numTritsAvailable, int numTritsToConvert);
    }
}
