﻿using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Http.Connections;

namespace zero.core.data.providers.cassandra
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

        public long GetPartition(long value)
        {
            return (long) Math.Truncate(value / (double) PartitionSize) * PartitionSize;
        }

        public long[] GetPartitionSet(long value)
        {
            return new[] { value - PartitionSize, value, value + PartitionSize};
        }
    }
}