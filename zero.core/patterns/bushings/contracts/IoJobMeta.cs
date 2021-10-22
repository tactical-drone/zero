namespace zero.core.patterns.bushings.contracts
{
    public class IoJobMeta
    {
        /// <summary>
        /// Respective States as the work goes through the source consumer pattern
        /// </summary>
        public enum JobState
        {
            Undefined,
            Producing,
            Produced,
            ProStarting,
            Queued,
            Consuming,
            Consumed,
            ConInlined,
            Error,
            Race,
            Accept,
            Reject,
            Halted,
            Syncing,
            RSync,
            ProduceErr,
            ConsumeErr,
            DbError,
            ConInvalid,
            NoPow,
            FastDup,
            SlowDup,
            ConCancel,
            ProdCancel,
            ConsumeTo,
            ProduceTo,
            Cancelled,
            Timeout,
            Oom,
            Zeroed
        }
    }
}
