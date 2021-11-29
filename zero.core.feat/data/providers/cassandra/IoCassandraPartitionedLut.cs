using System;

namespace zero.core.feat.data.providers.cassandra
{
    public abstract class IoCassandraPartitionedLut
    {
        private long _partition;
        public long Partition
        {
            get => _partition;
            set => _partition = GetPartition(value);
        }
        public abstract long PartitionSize { get; }

        public volatile bool HasChanges;

        public volatile bool Loaded;

        public long GetPartition(long value)
        {
            return (long) Math.Truncate(value / (double) PartitionSize) * PartitionSize;
        }

        public long[] GetPartitionSet(long value)
        {
            var partition = GetPartition(value);
            return new[] { partition - PartitionSize, partition, partition + PartitionSize};
        }
    }
}
