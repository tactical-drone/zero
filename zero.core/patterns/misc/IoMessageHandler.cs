using System;
using System.Threading;
using zero.core.patterns.heap;
using zero.core.patterns.schedulers;
using NLog;
using zero.core.models;
using zero.core.patterns.bushes;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Marshalls messages from a message source into a job queue
    /// </summary>
    /// <typeparam name="TSource">The type of the source generating the messages</typeparam>
    public abstract class IoMessageHandler<TSource> : IoProducerConsumer<IoMessage<TSource>, TSource> 
        where TSource : IoConcurrentProcess
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="description">A Description of this message handler</param>
        /// <param name="mallocMessage">Job memory allocation hook</param>
        protected IoMessageHandler(string description, Func<IoMessage<TSource>> mallocMessage) :base(description, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Logger
        /// </summary>
        private Logger _logger;

        /// <summary>
        /// A description of this message handler
        /// </summary>
        protected string StreamDescriptor = nameof(IoMessageHandler<TSource>);

        /// <summary>
        /// The total number of bytes received from this neighbor
        /// </summary>
        protected long TotalBytesReceived = 0;

        /// <summary>
        /// The total number of messages seen by this message handler
        /// </summary>
        protected long TotalMessagesCount = 0;

        /// <summary>
        /// Process a job
        /// </summary>
        /// <param name="currJob">The current job fragment to be procesed</param>
        /// <param name="currJobPreviousFragment">Include a previous job fragment if job spans two productions</param>
        protected override IoProducable<TSource>.State Consume(IoMessage<TSource> currJob, IoMessage<TSource> currJobPreviousFragment = null)
        {
            //Wait for the producer to release this consumer
            var retval = currJob.ConsumeBarrier();

            //Did the producer produce anything?
            if (retval == IoProducable<TSource>.State.Consuming)
                Interlocked.Add(ref TotalBytesReceived, currJob.BytesRead);
            
            //Store previous job datum fragment size for calculations below
            var previousFragmentByteLength = currJobPreviousFragment?.BytesLeftToProcess??0;

            //Calculate the number of datums available for processing (including previous fragments)
            var totalBytesAvailable = currJob.BytesLeftToProcess + previousFragmentByteLength;
            currJob.DatumCount = totalBytesAvailable / currJob.DatumLength;
            currJob.DatumFragmentLength = totalBytesAvailable % currJob.DatumLength;
            
            //Copy a previous job buffer fragment into the current job buffer
            if (currJobPreviousFragment != null)
            {
                Array.Copy(currJobPreviousFragment.Buffer, currJobPreviousFragment.BufferOffset, currJob.Buffer, currJob.BufferOffset - previousFragmentByteLength, previousFragmentByteLength);

                //Update buffer pointers
                currJob.BufferOffset -= previousFragmentByteLength;
                currJob.BytesRead += previousFragmentByteLength;
            }            

            Interlocked.Add(ref TotalMessagesCount, (long)currJob.DatumCount);
            return retval;
        }
    }
}
