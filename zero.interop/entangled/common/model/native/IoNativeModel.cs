using System;
using System.Text;
using Tangle.Net.Entity;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.mock;
using zero.interop.entangled.mock;

namespace zero.interop.entangled.common.model.native
{
    class IoNativeModel : IIoInteropModel
    {        
        public IIoInteropTransactionModel GetTransaction(sbyte[] tritBuffer, int buffOffset = 0, sbyte[] tryteBuffer = null, int length = 0)
        {
            IoEntangled.Default.Trinary.GetTrytes(tritBuffer, buffOffset, tryteBuffer, tritBuffer.Length);

            var tx = IoMockTransaction.FromTrytes(new TransactionTrytes(Encoding.ASCII.GetString((byte[])(Array)tryteBuffer)));
            var interopTransaction = new IoNativeTransactionModel
            {                               
                Message = tx.Fragment.Value,
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

            return interopTransaction;            
        }
    }
}
