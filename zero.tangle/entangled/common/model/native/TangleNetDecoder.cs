using System;
using System.Text;
using Tangle.Net.Entity;
using zero.core.misc;
using zero.core.models;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using zero.interop.entangled.native;
using zero.tangle.models;
using zero.tangle.utils;

namespace zero.tangle.entangled.common.model.native
{
    /// <summary>
    /// A native C# model decoder used for mocking <see cref="IIoModelDecoder{TKey}"/>
    /// </summary>
    internal class TangleNetDecoder : IIoModelDecoder<string>
    {
        
        public IIoTransactionModelInterface GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {
            var tryteBuffer = new sbyte[IoTransaction.NUM_TRYTES_SERIALIZED_TRANSACTION];
            var tryteHashByteBuffer = new sbyte[IoTransaction.NUM_TRYTES_HASH];

            Entangled<string>.Default.Ternary.GetTritsFromFlexTrits(flexTritBuffer, buffOffset, tritBuffer, Codec.MessageSize);
            Entangled<string>.Default.Ternary.GetTrytesFromTrits(tritBuffer, 0, tryteBuffer, IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION);

            var tx = IoMockTransaction.FromTrytes(new TransactionTrytes(Encoding.UTF8.GetString(new ReadOnlySpan<byte>((byte[])(Array)tryteBuffer))));
                        
            var interopTransaction = new TangleNetTransaction
            {
                SignatureOrMessageBuffer = Encoding.UTF8.GetBytes(tx.Fragment.Value.Trim('9')),
                AddressBuffer = Encoding.UTF8.GetBytes(tx.Address.Value),
                Value = tx.Value,
                ObsoleteTagBuffer = Encoding.UTF8.GetBytes(tx.ObsoleteTag.Value.AsMemory().ToArray()),
                Timestamp = tx.Timestamp.NormalizeDateTime(),
                CurrentIndex = tx.CurrentIndex,
                LastIndex = tx.LastIndex,
                BundleBuffer = Encoding.UTF8.GetBytes(tx.BundleHash.Value.AsMemory().ToArray()),
                TrunkBuffer = Encoding.UTF8.GetBytes(tx.TrunkTransaction.Value.AsMemory().ToArray()),
                BranchBuffer = Encoding.UTF8.GetBytes(tx.BranchTransaction.Value.AsMemory().ToArray()),                                                
                TagBuffer = Encoding.UTF8.GetBytes(tx.Tag.Value.Trim('9').AsMemory().ToArray()),
                AttachmentTimestamp = tx.AttachmentTimestamp.NormalizeDateTime(),
                AttachmentTimestampLower = tx.AttachmentTimestampLowerBound.NormalizeDateTime(),
                AttachmentTimestampUpper = tx.AttachmentTimestampUpperBound.NormalizeDateTime(),                
                NonceBuffer = Encoding.UTF8.GetBytes(tx.Nonce.Value.Trim('9').AsMemory().ToArray()),
                HashBuffer = Encoding.UTF8.GetBytes(tx.Hash.Value.AsMemory().ToArray()),                
                Blob = ((byte[])(Array)tryteBuffer)
            };

            interopTransaction.PopulateTotalSize();

            //check pow
            Entangled<string>.Default.Ternary.GetTrytesFromTrits(tritBuffer, IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION + 1, tryteHashByteBuffer, IoTransaction.NUM_TRITS_HASH - 9);

            var proposedHash = new Hash(Encoding.ASCII.GetString((byte[])(Array)tryteHashByteBuffer)).Value;
            Pow<string>.Compute((IIoTransactionModel<string>) interopTransaction, Encoding.UTF8.GetString(interopTransaction.HashBuffer.Span) , proposedHash);

            return interopTransaction;            
        }
    }
}
