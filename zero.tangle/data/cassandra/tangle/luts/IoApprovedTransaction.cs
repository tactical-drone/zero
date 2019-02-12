﻿using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds a transaction's verifier
    /// </summary>
    /// <typeparam name="TBlob">Type of blob field</typeparam>
    public class IoApprovedTransaction<TBlob>:IoCassandraPartitionedLut
    {
        public override long PartitionSize => 3600000;
        public TBlob Hash { get; set; }
        public TBlob Verifier { get; set; }                
        public sbyte Pow { get; set; }        
        public long MilestoneIndexEstimate { get; set; }        
    }
}
