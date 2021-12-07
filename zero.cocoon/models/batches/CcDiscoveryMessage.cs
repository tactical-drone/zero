using System.Net;
using Google.Protobuf;
using Zero.Models.Protobuf;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryMessage
    {
        public volatile IMessage EmbeddedMsg;
        public volatile chroniton Message;
        public volatile byte[] EndPoint;
    }
}
