using System;
using zero.core.models;

namespace zero.interop.entangled.common.model.interop
{
    public static class IoMarshalTransactionDecoder
    {
        public static unsafe IIoTransactionModelInterface Deserialize(sbyte[] flexTritBuffer, int buffOffset, bool computeHash, Func<IoMarshalledTransaction, IIoTransactionModelInterface> transform)
        {
            fixed (sbyte* flexTrits = &flexTritBuffer[buffOffset])
            {
                IoTransaction.transaction_deserialize_from_trits(out var memMap, flexTrits, computeHash);

                return transform(memMap);               
            }
        }
    }
}
