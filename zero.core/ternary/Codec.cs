using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace zero.core.ternary
{
    /// <summary>
    /// A helper class to encode and decode ternary ecodings
    /// </summary>
    public class Codec
    {
        /// <summary>
        /// Constructs some lookup tables
        /// </summary>
        static Codec()
        {
            Init();
        }

        /// <summary>
        /// The size of the tryte symbol alpabet
        /// </summary>
        private const int AlphabetLength = 27;

        /// <summary>
        /// The symbols used for the tryte alphabet
        /// </summary>
        private static readonly char[] Alphabet = { '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

        /// <summary>
        /// The radix of ternary
        /// </summary>
        public const int Radix = 3;

        /// <summary>
        /// The maximum value of a trit represented as an integer
        /// </summary>
        public const int MaxTrit = (Radix - 1) / 2;

        /// <summary>
        /// The minimum value of a trit represented as an integer
        /// </summary>
        public const int MinTrit = -MaxTrit;

        /// <summary>
        /// The number of trits can you pack into a byte
        /// </summary>
        public const int TritsPerByte = 5;

        /// <summary>
        /// The number of trits can you pack into a tryte
        /// </summary>
        public const int TritsPerTryte = 3;

        /// <summary>
        /// A Lookup for trits from bytes
        /// </summary>
        private static readonly sbyte[][] ByteLookupTritTable = new sbyte[(int)Math.Pow(TritsPerTryte, TritsPerByte)][];

        /// <summary>
        /// A lookup for trytes from trits
        /// </summary>
        private static readonly sbyte[][] TryteLookupTritTable = new sbyte[(int)Math.Pow(Radix, Radix)][];

        /// <summary>
        /// Initialized lookup tables used in decoding
        /// </summary>
        public static void Init()
        {
            var map = new sbyte[TritsPerByte];

            for (var i = 0; i < (Math.Pow(TritsPerTryte, TritsPerByte)); i++)
            {
                ByteLookupTritTable[i] = map.Take(TritsPerByte).ToArray();
                Increment(map, TritsPerByte);
            }

            for (var i = 0; i < (Math.Pow(Radix, Radix)); i++)
            {
                TryteLookupTritTable[i] = map.Take(TritsPerTryte).ToArray();
                Increment(map, TritsPerTryte);
            }
        }

        /// <summary>
        /// Increment a list of trits by one
        /// </summary>
        /// <param name="trits">An array of trits that are to be incremented by 1</param>
        /// <param name="length">The number of trits in the array to increment by 1</param>
        private static void Increment(IList<sbyte> trits, int length)
        {
            for (var i = 0; i < length; i++)
            {
                if (++trits[i] > Codec.MaxTrit)
                    trits[i] = (sbyte)Codec.MinTrit;
                else
                    break;
            }
        }

        /// <summary>
        /// Takes in a array of bytes and decodes it into a array of trits
        /// </summary>
        /// <param name="buffer">The buffer containing the bytes that is to be decoded</param>
        /// <param name="buffOffset">The offset into the buffer to start the decoding</param>
        /// <param name="tritBuffer">The buffer to store the decoded trits into</param>
        /// <param name="length">The number of bytes in the array to convert</param>        
        public static void GetTrits(sbyte[] buffer, int buffOffset, sbyte[] tritBuffer, int length)
        {
            var curPos = 0;

            for (var i = 0; i < length && curPos < tritBuffer.Length; i++)
            {
                Array.Copy(ByteLookupTritTable[buffer[i + buffOffset] < 0 ? (buffer[i + buffOffset] + ByteLookupTritTable.Length) : buffer[i + buffOffset]], 0, tritBuffer, curPos, tritBuffer.Length - curPos < TritsPerByte ? (tritBuffer.Length - curPos) : TritsPerByte);
                curPos += TritsPerByte;
            }

            while (curPos < tritBuffer.Length)
                tritBuffer[curPos++] = 0;
        }

        /// <summary>
        /// Converts an array of trits into an array of trytes
        /// </summary>
        /// <param name="buffer">The trit buffer</param>
        /// <param name="offset">The offset into the buffer to start reading from</param>
        /// <param name="trytes">A buffer containing the result of the decoded trits</param>
        /// <param name="length">The number of trits to convert</param>
        /// <returns></returns>
        public static void GetTrytes(sbyte[] buffer, int offset, StringBuilder trytes, int length)
        {
            trytes.Clear();            
            for (var i = 0; i < (length + TritsPerTryte - 1) / TritsPerTryte; i++)
            {
                var j = buffer[offset + i * 3] + buffer[offset + i * 3 + 1] * 3 + buffer[offset + i * 3 + 2] * 9;
                if (j < 0)
                    j += AlphabetLength;
                trytes.Append(Alphabet[j]);
            }
        }
    }
}
