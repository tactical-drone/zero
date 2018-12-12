using System;
using System.Collections.Generic;
using System.Text;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.abstraction
{
    public interface IIoInteropModel
    {
        IIoInteropTransactionModel GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null);
    }
}
