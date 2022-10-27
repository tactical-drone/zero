using System.Net;
using System.Threading;
using zero.core.feat.models.bundle;
using zero.core.misc;
using Zero.Models.Protobuf;

namespace zero.cocoon.models.batches
{
    public class CcBatchMessage :IIoBundleMessage
    {
        private chroniton _zero;
        public chroniton Zero { get => _zero; set => Interlocked.Exchange(ref _zero, value); }
        public byte[] EndPoint { get; protected set; } = new IPEndPoint(IPAddress.Any, 0).AsBytes();
    }
}
