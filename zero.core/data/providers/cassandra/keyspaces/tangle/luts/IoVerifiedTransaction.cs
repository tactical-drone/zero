namespace zero.core.data.luts
{    
    /// <summary>
    /// Finds a transaction's verifier
    /// </summary>
    /// <typeparam name="TBlob">Type of blob field</typeparam>
    public class IoVerifiedTransaction<TBlob>
    {
        public TBlob Hash { get; set; }
        public TBlob Verifier { get; set; }
        public TBlob Trunk { get; set; }
        public TBlob Branch { get; set; }
        public long Timestamp { get; set; }
        public sbyte Pow { get; set; }
    }
}
