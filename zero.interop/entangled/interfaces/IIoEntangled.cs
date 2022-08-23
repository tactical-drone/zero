using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary.interop;

namespace zero.interop.entangled.interfaces
{
    /// <summary>
    /// Main interface when decoding transaction data
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface IIoEntangled<TKey>
    {
        IIoTrinary Ternary { get; }
        IIoModelDecoder<TKey> ModelDecoder { get; }
    }
}
