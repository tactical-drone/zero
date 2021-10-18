using Google.Protobuf;
using Proto;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryMessage
    {
        public volatile IMessage EmbeddedMsg;
        public volatile Packet Message;
    }
}
