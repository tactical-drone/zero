using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary.abstraction;
using zero.interop.entangled.common.trinary.interop;

namespace zero.interop.entangled.interfaces
{
    public interface IIoEntangledInterop 
    {
        IIoTrinary Trinary { get; }
        IIoInteropModel Model { get; }
    }
}
