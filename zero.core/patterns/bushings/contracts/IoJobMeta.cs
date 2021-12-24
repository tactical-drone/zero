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
            ZeroRecovery,
            Consumed,
            ConInlined,
            Error,
            Race,
            Accept,
            Reject,
            Halted,
            DeSynced,
            Fragmented,
            Recovery,
            RSync,
            ProduceErr,
            ConsumeErr,
            DbError,
            BadData,
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
