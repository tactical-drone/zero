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
        public override long PartitionSize => 200000;
        public TKey Hash { get; set; }
        public TKey Verifier { get; set; }
        public TKey TrunkBranch { get; set; }
        public long Balance { get; set; }
        public sbyte Pow { get; set; }
        public long Timestamp { get; set; }

        long _confirmationTime;
        public long ConfirmationTime
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

        private long _height = long.MaxValue;
        public long Height
        {
            get => Volatile.Read(ref _height);
            set => Volatile.Write(ref _height, value);
        }

        
        private long _depth = 0;
        public long Depth
        {
            get => Volatile.Read(ref _depth);
            set => Volatile.Write(ref _depth, value);
        }

        //Graph meta
        public volatile bool Walked;        
    }
}
