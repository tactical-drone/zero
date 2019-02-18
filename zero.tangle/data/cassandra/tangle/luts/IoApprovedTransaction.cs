using System.Runtime.Serialization;
using System.Threading;
using zero.core.data.providers.cassandra;

namespace zero.tangle.data.cassandra.tangle.luts
{    
    /// <summary>
    /// Finds a transaction's verifier
    /// </summary>
    /// <typeparam name="TKey">Type of key field</typeparam>
    public class IoApprovedTransaction<TKey>:IoCassandraPartitionedLut
    {
        public override long PartitionSize => 500000;
        public TKey Hash { get; set; }
        public TKey Verifier { get; set; }
        public TKey TrunkBranch { get; set; }
        public long Balance { get; set; }
        public sbyte Pow { get; set; }
        public long Timestamp { get; set; }
        public long SecondsToMilestone { get; set; }

        public long MilestoneIndexEstimate
        {
            get => Interlocked.Read(ref _milestoneIndexEstimate);
            set => Volatile.Write(ref _milestoneIndexEstimate, value);
        }
        public bool IsMilestone { get; set; }

        public long Depth
        {
            get => Interlocked.Read(ref _depth);
            set => Volatile.Write(ref _depth, value);
        }

        //Graph walking variables
        [IgnoreDataMember] public volatile bool Walked;
        [IgnoreDataMember] public volatile bool Loaded;
        [IgnoreDataMember] private long _depth;
        [IgnoreDataMember] private long _milestoneIndexEstimate;        
    }
}
