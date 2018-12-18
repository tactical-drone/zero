using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary.interop;

namespace zero.interop.entangled.interfaces
{
    public interface IIoEntangledInterop<TBlob> 
    {
        IIoTrinary Ternary { get; }
        IIoInteropModel<TBlob> Model { get; }
    }
}
