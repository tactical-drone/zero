using zero.core.data.contracts;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoProducer
    {
        /// <summary>
        /// Keys this instance.
        /// </summary>
        /// <returns>The unique key of this instance</returns>
        string Key { get; }

        /// <summary>
        /// Description of the producer
        /// </summary>
        string Description { get; }

        /// <summary>
        /// Source URI
        /// </summary>
        string SourceUri { get; }

        /// <summary>
        /// Gets a value indicating whether this instance is operational.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is operational; otherwise, <c>false</c>.
        /// </value>
        bool IsOperational { get; }

        /// <summary>
        /// Used to identify work that was done recently
        /// </summary>
        IIoDupChecker RecentlyProcessed { get; set; }
    }
}
