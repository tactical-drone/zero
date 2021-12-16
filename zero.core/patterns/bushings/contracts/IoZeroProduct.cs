﻿using System;
using System.Threading.Tasks;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushings.contracts
{
    /// <summary>
    /// Fake work
    /// </summary>
    /// <seealso cref="IIoJob" />
    public class IoZeroProduct: IoSink<IoZeroProduct>
    {
        private volatile bool _produced;
        private volatile bool _consumed;

        /// <summary>
        /// The time it takes to manufacture this production
        /// </summary>
        private int _constructionDelay;

        public bool Produced => _produced;
        public bool Consumed => _consumed;

        public IoZeroProduct(string description, IoSource<IoZeroProduct> source, int constructionDelay = 1000) : base($"{nameof(IoZeroProduct)}:{description}", "stub", source, source.ZeroConcurrencyLevel())
        {
            _constructionDelay = constructionDelay;
        }

        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(IIoSource.IoZeroCongestion<T> barrier,
            T ioZero)
        {
            if (!await Source.ProduceAsync(static async (_, backPressure, state, ioJob) =>
                {
                    var job = (IoZeroProduct)ioJob;

                    if (!await backPressure(ioJob, state).FastPath().ConfigureAwait(job.Zc))
                        return false;

                    //mock production delay
                    await Task.Delay(job._constructionDelay);

                    return job._produced = true;
                }, this, barrier, ioZero).FastPath().ConfigureAwait(Zc))
            {
                return State = IoJobMeta.JobState.Error;
            }

            return State = IoJobMeta.JobState.Produced;
        }

        public override async ValueTask<IIoHeapItem> ReuseAsync()
        {
            await base.ReuseAsync();

            //user safety, rtfm?/rtfc!
            State = IoJobMeta.JobState.Undefined;

            return this;
        }

        public override ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            _consumed = true;
            return new ValueTask<IoJobMeta.JobState>(State = IoJobMeta.JobState.Consumed);
        }

        public override void SyncPrevJob()
        {
            
        }

        public override void JobSync()
        {
            
        }
    }
}
