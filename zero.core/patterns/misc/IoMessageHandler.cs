﻿using System;
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
        /// Constructir
        /// </summary>
        /// <param name="description">A Description of this message handler</param>
        /// <param name="mallocMessage">Job memory allocation hook</param>
        /// <param name="jobThreadScheduler">The thread scheduler used to process work on</param>
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

        protected long TotalBytesReceived = 0;

        /// <summary>
        /// Process a job
        /// </summary>
        /// <param name="currJob">The current job fragment to be procesed</param>
        /// <param name="currJobPreviousFragment">Include a previous job fragment if job spans two productions</param>
        protected override IoProducable<TSource>.State Consume(IoMessage<TSource> currJob, IoMessage<TSource> currJobPreviousFragment = null)
        {
            //Consume this work
            var retval = currJob.Consume();

            if (retval == IoProducable<TSource>.State.Consuming)
                Interlocked.Add(ref TotalBytesReceived, currJob.BytesRead);

            return retval;
        }
    }
}
