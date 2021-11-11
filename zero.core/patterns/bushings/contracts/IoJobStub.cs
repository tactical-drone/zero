﻿using System;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings.contracts
{
    /// <summary>
    /// Empty worker stub used to signal that a consumer should not forward jobs
    /// </summary>
    /// <seealso cref="IIoJob" />
    public class IoJobStub: IoNanoprobe, IIoJob
    {
        private readonly string _description = "Job Stub";
        public override string Description => _description;
        public long Id { get; } = -1;
        public IoJobMeta.JobState FinalState { get; set; }
        public IoJobMeta.JobState State { get; set; }
        public IIoJob PreviousJob { get; } = null;
        public IIoSource Source { get; } = null;
        public bool Syncing { get; } = false;

        public ValueTask<IoJobMeta.JobState> ProduceAsync(Func<IIoJob, IIoZero, ValueTask<bool>> barrier, IIoZero zeroClosure)
        {
            throw new NotImplementedException();
        }

        public ValueTask<IIoHeapItem> ConstructorAsync()
        {
            throw new NotImplementedException();
        }

        public IoJobStub(string description) : base($"{nameof(IoJobStub)}:{description}", 0)
        {
        }
    }
}