﻿using System;
using System.Threading.Tasks;
using zero.core.misc;
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
        private readonly int _constructionDelay;

        public bool Produced => _produced;
        public bool Consumed => _consumed;

        /// <summary>
        /// sentinel
        /// </summary>
        public IoZeroProduct()
        {
            
        }

        public IoZeroProduct(string description, IoSource<IoZeroProduct> source, int constructionDelay = 1000) : base($"{nameof(IoZeroProduct)}:{description}", "stub", source, source.ZeroConcurrencyLevel)
        {
            _constructionDelay = constructionDelay;
        }
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(IIoSource.IoZeroCongestion<T> barrier,
            T ioZero)
        {
            if (await Source.ProduceAsync(static async (source, backPressure, state, ioJob) =>
                {
                    var job = (IoZeroProduct)ioJob;

                    if (!await backPressure(state).FastPath())
                        return await ioJob.SetStateAsync(IoJobMeta.JobState.Error);

                    //mock production delay
                    if (job._constructionDelay > 0)
                        await Task.Delay(job._constructionDelay);

                    job.GenerateJobId();

                    if (job._produced = ((IoZeroSource)source).Produce())
                        return await ioJob.SetStateAsync(IoJobMeta.JobState.Produced);
                    else
                        return await ioJob.SetStateAsync(IoJobMeta.JobState.ProdSkipped);

                }, this, barrier, ioZero).FastPath() != IoJobMeta.JobState.Produced)
            {
                return State;
            }

            return State;
        }

        public override async ValueTask<IIoHeapItem> HeapPopAsync(object context)
        {
            await base.HeapPopAsync(context).FastPath();

            //user safety, rtfm?/rtfc!
            await SetStateAsync(IoJobMeta.JobState.Undefined).FastPath();

            return this;
        }

        public override ValueTask<IoJobMeta.JobState> ConsumeAsync()
        {
            _consumed = true;
            return SetStateAsync (IoJobMeta.JobState.Consumed);
        }

        protected internal override ValueTask AddRecoveryBitsAsync()
        {
            throw new NotImplementedException();
        }

        protected internal override bool ZeroEnsureRecovery()
        {
            throw new NotImplementedException();
        }
    }
}
