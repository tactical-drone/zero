using System;

namespace zero.core.data.luts
{    
    /// <summary>
    /// Finds a transaction's verifier
    /// </summary>
    /// <typeparam name="TBlob">Type of blob field</typeparam>
    public class IoVerifiedTransaction
    {
        public ReadOnlyMemory<byte> Hash { get; set; }
        public ReadOnlyMemory<byte> Verifier { get; set; }
        public long Timestamp { get; set; }
        public sbyte Pow { get; set; }
    }
}
