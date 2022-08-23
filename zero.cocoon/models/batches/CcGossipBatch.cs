using zero.core.patterns.misc;
using Zero.Models.Protobuf;

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
