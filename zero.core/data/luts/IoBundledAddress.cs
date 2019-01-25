using System;

namespace zero.core.data.luts
{
    /// <summary>
    /// Gets the bundle from address
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoBundledAddress
    {                
        public ReadOnlyMemory<byte> Address { get; set; }
        
        public ReadOnlyMemory<byte> Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
