using System.Text;

namespace zero.interop.entangled.common.trinary.abstraction
{
    public interface IIoTrinary
    {
        void GetTrits(sbyte[] buffer, int buffOffset, sbyte[] tritBuffer, int length);

        /// <summary>
        /// Converts an array of trits into an array of trytes
        /// </summary>
        /// <param name="tritBuffer">The trit buffer</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="tryteBuffer">A buffer containing the result of the decoded trits</param>
        /// <param name="length">The number of trits to convert</param>
        void GetTrytes(sbyte[] tritBuffer, int offset, sbyte[] tryteBuffer, int length);
    }
}
