using System;
using System.Collections.Generic;
using System.Text;
using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{
    public class IoMilestoneTransaction<TBlob> : IoCassandraPartitionedLut
    {
        public override long PartitionSize => 3600000;
        public TBlob Hash { get; set; }
        public TBlob Bundle { get; set; }
        public TBlob ObsoleteTag { get; set; }
        public long Timestamp { get; set; }        
        public long MilestoneIndex { get; set; }
    }
}
