﻿using System;
using System.Text;
using Tangle.Net.Entity;
using zero.core.misc;
using zero.core.models;
using zero.interop.entangled;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using zero.interop.entangled.mock;
using zero.interop.utils;

namespace zero.tangle.entangled.common.model.native
{
    /// <summary>
    /// A native C# model decoder used for mocking <see cref="IIoModelDecoder{TBlob}"/>
    /// </summary>
    internal class IoNativeModelDecoder : IIoModelDecoder<string>
    {
        
        public IIoTransactionModelInterface GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null)
        {
            var tryteBuffer = new sbyte[IoTransaction.NUM_TRYTES_SERIALIZED_TRANSACTION];
            var tryteHashByteBuffer = new sbyte[IoTransaction.NUM_TRYTES_HASH];

            IoEntangled<string>.Default.Ternary.GetTritsFromFlexTrits(flexTritBuffer, buffOffset, tritBuffer, Codec.MessageSize);
            IoEntangled<string>.Default.Ternary.GetTrytesFromTrits(tritBuffer, 0, tryteBuffer, IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION);

            var tx = IoMockTransaction.FromTrytes(new TransactionTrytes(Encoding.UTF8.GetString(new ReadOnlySpan<byte>((byte[])(Array)tryteBuffer))));

            var obsoleteTag = tx.ObsoleteTag.Value.TrimEnd('9');
            if (string.IsNullOrEmpty(obsoleteTag))            
                obsoleteTag = "";
            
            var interopTransaction = new IoNativeTransactionModel
            {
                SignatureOrMessageBuffer = Encoding.UTF8.GetBytes(tx.Fragment.Value.Trim('9')),
                AddressBuffer = Encoding.UTF8.GetBytes(tx.Address.Value),
                Value = tx.Value,
                ObsoleteTagBuffer = Encoding.UTF8.GetBytes(obsoleteTag.AsMemory().ToArray()),
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
                Snapshot = tx.Snapshot,
                MilestoneIndexEstimate = tx.SnapshotIndex,
                Solid = tx.Solid,
                Blob = ((byte[])(Array)tryteBuffer),

            };
            interopTransaction.Size = (short) (interopTransaction.SignatureOrMessageBuffer.Length
                                               + interopTransaction.AddressBuffer.Length
                                               + sizeof(long)
                                               + interopTransaction.ObsoleteTagBuffer.Length
                                               + sizeof(long)
                                               + sizeof(long)
                                               + sizeof(long)
                                               + interopTransaction.BundleBuffer.Length
                                               + interopTransaction.TrunkBuffer.Length
                                               + interopTransaction.BranchBuffer.Length
                                               + interopTransaction.TagBuffer.Length
                                               + sizeof(long)
                                               + sizeof(long)
                                               + sizeof(long)
                                               + interopTransaction.NonceBuffer.Length
                                               + interopTransaction.HashBuffer.Length);

            //check pow
            IoEntangled<string>.Default.Ternary.GetTrytesFromTrits(tritBuffer, IoTransaction.NUM_TRITS_SERIALIZED_TRANSACTION + 1, tryteHashByteBuffer, IoTransaction.NUM_TRITS_HASH - 9);

            var proposedHash = new Hash(Encoding.ASCII.GetString((byte[])(Array)tryteHashByteBuffer)).Value;
            IoPow<string>.Compute((IIoTransactionModel<string>) interopTransaction, Encoding.UTF8.GetString(interopTransaction.HashBuffer.Span) , proposedHash);

            return interopTransaction;            
        }
    }
}
