using System.Linq;
using System.Text;
using Tangle.Net.Entity;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.mock;
using zero.interop.utils;

namespace zero.interop.entangled.common.model.native
{
    internal class IoNativeModel : IIoInteropModel
    {        
        public IIoInteropTransactionModel GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {
            var tryteBuffer = new sbyte[IoTransaction.NUM_TRYTES_SERIALIZED_TRANSACTION];
            var tryteHashByteBuffer = new sbyte[IoTransaction.NUM_TRYTES_HASH];

            IoEntangled.Default.Trinary.GetTrits(flexTritBuffer, buffOffset, tritBuffer, Codec.MessageSize);
            IoEntangled.Default.Trinary.GetTrytes(tritBuffer, 0, tryteBuffer, IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION);
            
            var tx = IoMockTransaction.FromTrytes(new TransactionTrytes(Encoding.ASCII.GetString(tryteBuffer.Select(c => (byte)c).ToArray())));

            var interopTransaction = new IoNativeTransactionModel
            {                                               
                SignatureOrMessage = tx.Fragment.Value,
                Address = tx.Address.Value,
                Value = tx.Value,
                ObsoleteTag = tx.ObsoleteTag.Value,
                Timestamp = tx.Timestamp,
                CurrentIndex = tx.CurrentIndex,
                LastIndex = tx.LastIndex,
                Bundle = tx.BundleHash.Value,
                Trunk = tx.TrunkTransaction.Value,
                Branch = tx.BranchTransaction.Value,                                                
                Tag = tx.Tag.Value,
                AttachmentTimestamp = tx.AttachmentTimestamp,
                AttachmentTimestampLower = tx.AttachmentTimestampLowerBound,
                AttachmentTimestampUpper = tx.AttachmentTimestampUpperBound,                
                Nonce = tx.Nonce.Value,
                Hash = tx.Hash.Value,
                SnapshotIndex = tx.SnapshotIndex,
                Solid = tx.Solid                                
            };

            //check pow
            IoEntangled.Default.Trinary.GetTrytes(tritBuffer, IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION + 1, tryteHashByteBuffer, IoTransaction.NUM_TRITS_HASH - 9);

            var proposedHash = new Hash(Encoding.ASCII.GetString(tryteHashByteBuffer.Select(c => (byte)c).ToArray())).Value;
            var computedHash = interopTransaction.Hash;

            IIoInteropTransactionModel byref = interopTransaction;
            IoPow.Compute(ref byref, computedHash, proposedHash);

            return interopTransaction;            
        }
    }
}
