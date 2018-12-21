using System;
using System.Linq;
using Org.BouncyCastle.Crypto.Paddings;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;

namespace zero.interop.utils
{
    public static class IoPow
    {
        public const int MWM = 3;
        public static void Compute<TBlob>(IIoInteropTransactionModel<TBlob> transaction, string computedHash, string proposedHash) 
        {
            transaction.Pow = 0;
            transaction.ReqPow = 0;
            
            if (computedHash.Contains(proposedHash.Substring(0, 10)) &&
                computedHash.Substring(computedHash.Length - 11, 6)
                    .Equals(proposedHash.Substring(proposedHash.Length - 11, 6)))
            {
                for (var i = computedHash.Length - 1; i > 0 && computedHash[i--] == '9'; transaction.Pow++) { }

                if (transaction.Pow == IoTransaction.NUM_TRYTES_HASH)
                    transaction.Pow = 0;
            }
            else
            {
                for (var i = computedHash.Length - 1; i > 0 && computedHash[i--] == '9'; transaction.Pow--) { }
                transaction.ReqPow = transaction.Pow;
                for (var i = proposedHash.Length - 1; i > 0 && proposedHash[i--] == '9'; transaction.ReqPow++) { }

                if (transaction.Pow == -IoTransaction.NUM_TRYTES_HASH)
                {
                    transaction.Pow = 0;
                    transaction.ReqPow = (sbyte)-(transaction.ReqPow + IoTransaction.NUM_TRYTES_HASH);
                    if (transaction.ReqPow == IoTransaction.NUM_TRYTES_HASH)
                        transaction.ReqPow = 0;
                }
            }            
        }

        public static void ComputeFromBytes<TBlob>(IIoInteropTransactionModel<TBlob> transaction, byte[] computedHash, sbyte[] flexTritBuffer, int offset)
        {
            var proposedHash = flexTritBuffer.Skip(offset).Take(Codec.TransactionHashSize).ToArray();
            var minWM = 3;
            transaction.Pow = 0;
            transaction.ReqPow = (sbyte) minWM;
            
            for (var i = computedHash.Length; i-- > 0;)
            {
                if (computedHash[i] == 0)
                    transaction.Pow++;
                else
                    break;
            }
            
            if (transaction.Pow == computedHash.Length)
                transaction.Pow = 0;

            for (var i = Codec.TransactionHashSize; i-- > 0;)
            {
                if (proposedHash[i] == 0)
                    transaction.ReqPow++;
                else
                    break;
            }

            if (transaction.ReqPow - MWM == proposedHash.Length)
                transaction.ReqPow = 0;

            for (var i = Codec.TransactionHashSize; i-- > 0;)
            {
                if (proposedHash[i] != computedHash[i + minWM])
                {
                    transaction.ReqPow = (sbyte)-transaction.ReqPow;
                    break;
                }
            }

            //for (var i = 0; i < Codec.TransactionHashSize - 1; i++)
            //{
            //    if (proposedHash[i] != computedHash[i])
            //    {
            //        transaction.ReqPow = (sbyte)-transaction.ReqPow;
            //        break;
            //    }
            //}

            if (transaction.ReqPow > 0 )//|| transaction.ReqPow <= -minWM) //TODO remove debug stuff
            {
                Console.WriteLine("--------------------------------------------------------------------------------------------------------------");
                Console.WriteLine($"computed  = `{transaction.AsTrytes((TBlob)(object)computedHash)}'[{computedHash.Length}] --> {transaction.Pow}");
                Console.WriteLine($"Requested = `{transaction.AsTrytes((TBlob)(object)proposedHash)}'[{proposedHash.Length}] --> {transaction.ReqPow}");
            }
        }

    }
}
