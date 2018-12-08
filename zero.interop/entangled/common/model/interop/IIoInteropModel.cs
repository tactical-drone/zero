using System;
using System.Collections.Generic;
using System.Text;

namespace zero.interop.entangled.common.model.abstraction
{
    public interface IIoInteropModel
    {
        IIoInteropTransactionModel GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null);
    }
}
