using System.Net;
using Google.Protobuf;
using zero.core.misc;
using Zero.Models.Protobuf;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryMessage
    {
        public volatile IMessage EmbeddedMsg;
        public volatile chroniton Chroniton;
        public byte[] EndPoint { get; } = new IPEndPoint(IPAddress.Any, 0).AsBytes();
    }
}
