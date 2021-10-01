using System.Net;
using Google.Protobuf;
using Proto;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch
    {
        public IIoZero Zero;
        public IMessage EmbeddedMsg;
        public string RemoteEndPoint;
        public Packet Message;
        public IoHeap<CcDiscoveryBatch> HeapRef;
    }
}
