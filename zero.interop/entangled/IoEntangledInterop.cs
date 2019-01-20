using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled
{
    /// <summary>
    /// The optimized interop contract implementation
    /// </summary>
    public class IoEntangledInterop : IIoEntangled<byte[]>
    {                
        public IIoTrinary Ternary { get; } = new IoInteropTrinary();
        public IIoModelDecoder<byte[]> ModelDecoder { get; } = new IoInteropModelDecoder();
    }
}
