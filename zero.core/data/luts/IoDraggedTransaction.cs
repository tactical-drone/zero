namespace zero.core.data.lookups
{    
    public class IoDraggedTransaction<TBlob>
    {                
        public TBlob Address { get; set; }
        public TBlob Bundle { get; set; }

        public long AttachmentTimestamp;
        public long Timestamp;
        public long Value;
        public short Quality;

        public string Uri { get; set; }

        public float BtcValue;
        public float EthValue;
        public float EurValue;
        public float UsdValue;
    }
}
