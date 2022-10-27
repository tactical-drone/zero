using Zero.Models.Protobuf;

namespace zero.core.feat.models.bundle
{
    public interface IIoBundleMessage
    {
        public chroniton Zero { get; set; }
        public byte[] EndPoint { get; }
    }
}
