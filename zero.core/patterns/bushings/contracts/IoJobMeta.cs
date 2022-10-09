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
            ProdSkipped,
            ProdConnReset,
            ProduceErr,
            Queued,
            Consuming,
            ConInlined,
            Fragmented,
            BadData,
            ZeroRecovery,
            Recovering,
            Consumed,
            ConsumeErr,
            Error,
            Race,
            Accept,
            Reject,
            Halted,
            Synced,
            DeSynced,
            
            RSync,
            
            
            DbError,
            
            NoPow,
            FastDup,
            SlowDup,
            ConCancel,
            ProdCancel,
            ConsumeTo,
            
            Cancelled,
            Timeout,
            Oom,
            Zeroed
        }
    }
}
