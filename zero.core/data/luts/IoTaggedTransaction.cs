using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{    
    public class IoTaggedTransaction<TBlob>
    {                
        public TBlob Tag { get; set; }
        
        public TBlob Hash { get; set; }

        public long Timestamp { get; set; }
    }
}
