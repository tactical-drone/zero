using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.interfaces;
using zero.tangle.entangled.common.model;

namespace zero.tangle.entangled
{
    /// <summary>
    /// The optimized interop contract implementation
    /// </summary>
    public class EntangledInterop : IIoEntangled<byte[]>
    {                
        public IIoTrinary Ternary { get; } = new IoInteropTrinary();
        public IIoModelDecoder<byte[]> ModelDecoder { get; } = new EntangledDecoder();
    }
}
