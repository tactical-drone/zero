using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.interop;
using zero.interop.entangled.interfaces;

namespace zero.interop.entangled
{
    public class IoEntangledInterop : IIoEntangledInterop<byte[]>
    {                
        public IIoTrinary Ternary { get; } = new IoInteropTrinary();
        public IIoInteropModel<byte[]> Model { get; } = new IoInteropModel();
    }
}
