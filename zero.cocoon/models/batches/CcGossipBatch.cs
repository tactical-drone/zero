using Proto;
using zero.core.patterns.misc;

namespace zero.cocoon.models.batches
{ 
    public class CcGossipBatch:IoNanoprobe
    {
        public object UserData;
        public CcWhisperMsg Message;

        public override void ZeroUnmanaged()
        {
            UserData = null;
            Message = null;
            base.ZeroUnmanaged();
        }
    }
}
