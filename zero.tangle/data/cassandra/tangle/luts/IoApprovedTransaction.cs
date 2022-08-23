using System.Threading;
using zero.core.feat.data.providers.cassandra;

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

        int _confirmationTime;
        public int ConfirmationTime
        {
            get => _confirmationTime;
            set
            {
                if (value == _confirmationTime) return;
                _confirmationTime = value;
                HasChanges = true;
            }
        }

        private long _milestone;
        public long Milestone
        {
            get => Interlocked.Read(ref _milestone);
            set
            {
                if(Volatile.Read(ref _milestone) == value) return;
                Volatile.Write(ref _milestone, value);
                HasChanges = true;
            }
        }

        public bool IsMilestone { get; set; }

        //Graph meta
        public volatile int Height = int.MaxValue;
        public volatile int Depth = 0;        
        public volatile bool Walked;        
    }
}
