using System;

namespace zero.core.data.lookups
{    
    /// <summary>
    /// Stores where transactions are probably coming from
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoDraggedTransaction
    {                
        public ReadOnlyMemory<byte> Address { get; set; }
        public ReadOnlyMemory<byte> Bundle { get; set; }
        
        public long Timestamp;
        public long LocalTimestamp;
        public long AttachmentTimestamp;

        public long Value;
        public short Quality;

        public string Uri { get; set; }

        public float BtcValue;
        public float EthValue;
        public float EurValue;
        public float UsdValue;
    }
}
