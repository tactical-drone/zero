using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary.abstraction;

namespace zero.interop.entangled.interfaces
{
    public interface IIoEntangledInterop 
    {
        IIoTrinary Trinary { get; }
        IIoInteropModel Model { get; }
    }
}
