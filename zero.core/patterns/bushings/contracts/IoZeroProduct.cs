using System;
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
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(T ioZero)
        {
            if (!await Source.ProduceAsync(static async (source,  ioJob) =>
                {
                    var job = (IoZeroProduct)ioJob;

                    //mock production delay
                    if (job._constructionDelay > 0)
                        await Task.Delay(job._constructionDelay);

                    job.GenerateJobId();

                    return job._produced = ((IoZeroSource)source).Produce();
                }, this).FastPath())
            {
                return await SetStateAsync(IoJobMeta.JobState.ProduceErr);
            }

            return await SetStateAsync(IoJobMeta.JobState.Produced);
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
            return SetStateAsync (IoJobMeta.JobState.ConInlined);
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
