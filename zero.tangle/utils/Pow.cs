using System;
using System.Linq;
using zero.interop.entangled.common.model;
using zero.interop.entangled.mock;
using zero.tangle.models;

namespace zero.interop.utils
{
    /// <summary>
    /// Does pow calculation of <see cref="IIoTransactionModel{TKey}"/>"/>
    /// </summary>
    public static class Pow<TKey>
    {
        /// <summary>
        /// The minimum weight magnitude
        /// </summary>
        public const int MWM = 3;
        public static void Compute(IIoTransactionModel<TKey> transaction, string computedHash, string proposedHash) 
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

        /// <summary>
        /// Checks and compares pow
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="transaction">Transaction to be updated with pow results</param>
        /// <param name="computedHash">The hash that was computed</param>
        /// <param name="flexTritBuffer">The hash that was received from a neighbor</param>
        /// <param name="offset">The offset into the flex trit buffer</param>
        public static void ComputeFromBytes(IIoTransactionModel<TKey> transaction, ReadOnlyMemory<byte> computedHash, sbyte[] flexTritBuffer, int offset)
        {
            var proposedHash = flexTritBuffer.Skip(offset).Take(Codec.TransactionHashSize).ToArray();
            var minWM = 3;
            transaction.Pow = 0;
            transaction.ReqPow = (sbyte) minWM;
            
            for (var i = computedHash.Length; i-- > 0;)
            {
                if (computedHash.ToArray()[i] == 0)
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
                if (proposedHash[i] != computedHash.ToArray()[i + minWM])
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
                
                Console.WriteLine($"computed  = `{transaction.AsKeyString(computedHash)}'[{computedHash.Length}] --> {transaction.Pow}");
                Console.WriteLine($"Requested = `{transaction.AsKeyString((byte[])(Array)proposedHash)}'[{proposedHash.Length}] --> {transaction.ReqPow}");
            }
        }

    }
}
