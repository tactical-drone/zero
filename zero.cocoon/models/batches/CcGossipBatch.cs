using Proto;
using zero.core.patterns.bushes.contracts;

namespace zero.cocoon.models.batches
{ 
    public class CcGossipBatch
    {
        public IIoZero Zero;
        public object UserData;
        public CcWhisperMsg Message;
    }
}
