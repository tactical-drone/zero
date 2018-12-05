using System;
using System.Collections.Generic;
using System.Text;

namespace zero.interop.entangled.common.model.abstraction
{
    public interface IIoInteropModel
    {
        IIoInteropTransactionModel GetTransaction(sbyte[] tritBuffer, int buffOffset = 0, sbyte[] tryteBuffer = null, int length = 0);
    }
}
