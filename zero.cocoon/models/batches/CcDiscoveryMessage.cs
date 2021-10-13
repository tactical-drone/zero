using System.Threading.Tasks;
using Google.Protobuf;
using Proto;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryMessage:IoNanoprobe
    {
        public IIoZero Zero;
        public IMessage EmbeddedMsg;
        public string RemoteEndPoint;
        public Packet Message;
        public IoHeap<CcDiscoveryMessage> HeapRef;

        public override async ValueTask ZeroManagedAsync()
        {
            await base.ZeroManagedAsync().FastPath().ConfigureAwait(false);
            await HeapRef.ReturnAsync(this).FastPath().ConfigureAwait(false);
        }

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

            Zero = null;
            EmbeddedMsg = null;
            RemoteEndPoint = null;
            Message = null;
            HeapRef = null;
        }
    }
}
