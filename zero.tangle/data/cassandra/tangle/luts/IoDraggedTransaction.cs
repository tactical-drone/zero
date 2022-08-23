namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Stores where transactions are probably coming from
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public class IoDraggedTransaction<TKey>
    {                
        public TKey Address { get; set; }
        public TKey Bundle { get; set; }
        
        public long Timestamp;
        public long LocalTimestamp;
        public long AttachmentTimestamp;

        public long Value;
        public short Direction;
        public short Quality;

        public string Uri { get; set; }

        public float BtcValue;
        public float EthValue;
        public float EurValue;
        public float UsdValue;
    }
}
