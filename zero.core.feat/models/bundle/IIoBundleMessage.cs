using Zero.Models.Protobuf;

namespace zero.core.feat.models.bundle
{
    public interface IIoBundleMessage
    {
        public chroniton Payload { get; set; }
        public byte[] EndPoint { get; }
    }
}
