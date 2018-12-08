namespace zero.interop.entangled.common.trinary.interop
{
    public interface IIoTrinary
    {
        void GetTrits(sbyte[] flexTritsBuffer, int buffOffset, sbyte[] tritBuffer, int length);

        /// <summary>
        /// Converts an array of trits into an array of trytes
        /// </summary>
        /// <param name="tritBuffer">The trit buffer</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="tryteBuffer">A buffer containing the result of the decoded trits</param>
        /// <param name="numTritsToConvert">The number of trits to convert</param>
        void GetTrytes(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int numTritsToConvert);

        void GetFlexTrytes(sbyte[] tryteBuffer, int toLen, sbyte[] flexTritBuffer, int offset, int _, int numTritsToConvert);
    }
}
