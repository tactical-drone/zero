namespace zero.core.data.luts
{
    
    public class IoBundledAddress<TBlob>
    {                
        public TBlob Address { get; set; }
        
        public TBlob Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
