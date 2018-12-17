namespace zero.core.data.luts
{    
    public class IoVerifiedTransaction<TBlob>
    {
        public TBlob Hash { get; set; }
        public TBlob Verifier { get; set; }
        public long Timestamp { get; set; }
        public sbyte Pow { get; set; }
    }
}
