using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.abstraction
{
    public interface IIoInteropModel<TBlob> 
    {
        IIoInteropTransactionModel<TBlob> GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null);
    }
}
