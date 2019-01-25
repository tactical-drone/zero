using System;

namespace zero.core.data.lookups
{        
    /// <summary>
    /// Gets the bundle from transaction hash
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoBundledHash
    {        
        public ReadOnlyMemory<byte> Hash { get; set; }
        
        public ReadOnlyMemory<byte> Bundle { get; set; }

        public long Timestamp { get; set; }
    }
}
