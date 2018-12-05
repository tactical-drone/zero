namespace zero.interop.entangled.common.model.abstraction
{
    public class IoInteropModel : IIoInteropModel
    {
        public unsafe IIoInteropTransactionModel GetTransaction(sbyte[] tritBuffer, int buffOffset = 0, sbyte[] tryteBuffer = null, int length = 0)
        {
            fixed (sbyte* trits = tritBuffer)
            {
                return IoTransaction.transaction_deserialize(trits);
            }
        }
    }
}
