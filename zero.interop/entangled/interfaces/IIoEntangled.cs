using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.interop;

namespace zero.interop.entangled.interfaces
{
    /// <summary>
    /// Main interface when decoding transaction data
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public interface IIoEntangled<TBlob>
    {
        IIoTrinary Ternary { get; }
        IIoModelDecoder<TBlob> ModelDecoder { get; }
    }
}
