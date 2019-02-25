using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks.Dataflow;
using zero.core.misc;
using zero.core.models;
using zero.interop.entangled.mock;

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
