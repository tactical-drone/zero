using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.utils
{
    public static class IoPow
    {
        public static void Compute<TBlob>(IIoInteropTransactionModel<TBlob> transaction, string computedHash, string proposedHash) 
        {
            transaction.Pow = 0;
            transaction.FakePow = 0;

            if (computedHash.Contains(proposedHash.Substring(0, 10)) &&
                computedHash.Substring(computedHash.Length - 11, 6)
                    .Equals(proposedHash.Substring(proposedHash.Length - 11, 6)))
            {
                for (var i = computedHash.Length - 1; i > 0 && computedHash[i--] == '9'; transaction.Pow++) { }
            }
            else
            {
                for (var i = computedHash.Length - 1; i > 0 && computedHash[i--] == '9'; transaction.Pow--) { }
                transaction.FakePow = transaction.Pow;
                for (var i = proposedHash.Length - 1; i > 0 && proposedHash[i--] == '9'; transaction.FakePow++) { }

                if (transaction.Pow == -IoTransaction.NUM_TRYTES_HASH)
                {
                    transaction.Pow = 0;
                    transaction.FakePow = (sbyte)-(transaction.FakePow + IoTransaction.NUM_TRYTES_HASH);
                    if (transaction.FakePow == IoTransaction.NUM_TRYTES_HASH)
                        transaction.FakePow = 0;
                }
            }            
        }
          
    }
}
