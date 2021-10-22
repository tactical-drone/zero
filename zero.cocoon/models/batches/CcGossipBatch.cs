using Proto;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.cocoon.models.batches
{ 
    public class CcGossipBatch:IoNanoprobe
    {
        public IIoZero Zero;
        public object UserData;
        public CcWhisperMsg Message;

        public override void ZeroUnmanaged()
        {
            Zero = null;
            UserData = null;
            Message = null;
            base.ZeroUnmanaged();
        }
    }
}
