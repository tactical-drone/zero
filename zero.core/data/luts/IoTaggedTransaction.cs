using System;

namespace zero.core.data.luts
{    
    /// <summary>
    /// Finds transactions by tag
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoTaggedTransaction
    {                
        public ReadOnlyMemory<byte> Tag { get; set; }

        public ReadOnlyMemory<byte> ObsoleteTag { get; set; }

        public ReadOnlyMemory<byte> Hash { get; set; }

        public long Timestamp { get; set; }
    }
}
