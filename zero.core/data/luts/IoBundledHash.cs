using Cassandra.Mapping.Attributes;

namespace zero.core.data.lookups
{
    
    public class IoBundledHash<TBlob>
    {        
        public TBlob Hash { get; set; }
        
        public TBlob Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
