using System;
using System.Runtime.InteropServices;
using System.Text;
using NLog;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.interop.transaction;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.native;
using zero.interop.utils;
using zero.tangle.models;

// ReSharper disable InconsistentNaming

namespace zero.tangle.entangled.common.model
{
    /// <summary>
    /// Implements a interop transaction model
    /// </summary>
    public class EntangledTransaction : IIoTransactionModel<byte[]> //TODO base this
    {
        public EntangledTransaction()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private Logger _logger;
        public ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }        
        public byte[] SignatureOrMessage
        {
            get => SignatureOrMessageBuffer.AsArray();
            set => SignatureOrMessageBuffer = value;
        }

        public ReadOnlyMemory<byte> AddressBuffer { get; set; }
        public byte[] Address
        {
            get => AddressBuffer.AsArray();
            set => AddressBuffer = value;
        }

        public long Value { get; set; }
        public ReadOnlyMemory<byte> ObsoleteTagBuffer { get; set; }
        public byte[] ObsoleteTag
        {
            get => ObsoleteTagBuffer.AsArray();
            set => ObsoleteTagBuffer = new ReadOnlyMemory<byte>(value);
        }

        public long Timestamp { get; set; }
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
        public ReadOnlyMemory<byte> BundleBuffer { get; set; }
        public byte[] Bundle
        {
            get => BundleBuffer.AsArray();
            set => BundleBuffer = value;
        }

        public ReadOnlyMemory<byte> TrunkBuffer { get; set; }
        public byte[] Trunk
        {
            get => TrunkBuffer.AsArray();
            set => TrunkBuffer = value;
        }

        public ReadOnlyMemory<byte> BranchBuffer { get; set; }
        public byte[] Branch
        {
            get => BranchBuffer.AsArray();
            set => BranchBuffer = value;
        }

        public ReadOnlyMemory<byte> TagBuffer { get; set; }
        public byte[] Tag
        {
            get => TagBuffer.AsArray();            
            set => TagBuffer = value;
        }

        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> NonceBuffer { get; set; }
        public byte[] Nonce
        {
            get => NonceBuffer.AsArray();
            set => NonceBuffer = value;
        }

        public ReadOnlyMemory<byte> HashBuffer { get; set; }
        public byte[] Hash
        {
            get => HashBuffer.AsArray();
            set => HashBuffer = value;
        }

        public byte[] Snapshot { get; set; }

        public long MilestoneIndexEstimate { get; set; }
        public bool Solid { get; set; }
        public sbyte Pow { get; set; }
        public sbyte ReqPow { get; set; }
        public string Color
        {
            get
            {
                if (Pow == 0)
                    return "color: red";
                return Pow < 0 ? "color: orange" : "color:green";
            }
        }
        public string Uri { get; set; }
        public short Size { get; set; }
        public bool IsMilestoneTransaction { get; set; }
        public int ConfirmationTime { get; set; }
        public IIoTransactionModel<byte[]> MilestoneEstimateTransaction { get; set; }

        public ReadOnlyMemory<byte> Blob { get; set; }

        public string Key { get; set; }

        public string AsTrytes(ReadOnlyMemory<byte> field, int maxFlexTritsToConvert = IoTransaction.NUM_TRITS_HASH, int tryteLen = IoTransaction.NUM_TRYTES_HASH)
        {
            if (field.Length == 0)
                return string.Empty;

            var tritsToConvert = Math.Min(IoFlexTrit.NUM_TRITS_PER_FLEX_TRIT * field.Length, maxFlexTritsToConvert);
            var trytesToConvert = tritsToConvert / Codec.TritsPerTryte;
            tritsToConvert = trytesToConvert * Codec.TritsPerTryte;

            var trytes = new sbyte[trytesToConvert];
            //Console.Write($"[{trytes.Length},{tritsToConvert}]");
            Entangled<byte[]>.Default.Ternary.GetTrytesFromFlexTrits(trytes, trytes.Length, (sbyte[])(Array)field.ToArray(), 0, IoFlexTrit.NUM_TRITS_PER_FLEX_TRIT * field.Length, tritsToConvert);            
            return Encoding.ASCII.GetString((byte[])(Array)trytes);//.PadRight(tryteLen, '0');
        }

        public string AsKeyString(ReadOnlyMemory<byte> field, int maxFlexTritsToConvert = IoTransaction.NUM_TRITS_HASH, int tryteLen = IoTransaction.NUM_TRYTES_HASH)
        {
            return "0x" + BitConverter.ToString(field.AsArray()).Replace("-", "").ToLower().PadRight(98, '0');
        }

        public byte[] Trimmed(byte[] field, byte nullSet = 9)
        {
            return IoMarshalledTransaction.Trim(field, nullSet).AsArray();
        }

        public ReadOnlyMemory<byte> AsBlob()
        {
            return Blob;
        }

        public void PopulateTotalSize()
        {
            Size = (short)(SignatureOrMessageBuffer.Length 
                   + AddressBuffer.Length 
                   + Marshal.SizeOf(Value)
                   + ObsoleteTagBuffer.Length
                   + Marshal.SizeOf(Timestamp)
                   + Marshal.SizeOf(CurrentIndex)
                   + Marshal.SizeOf(LastIndex)
                   + BundleBuffer.Length
                   + TrunkBuffer.Length
                   + BranchBuffer.Length
                   + TagBuffer.Length
                   + Marshal.SizeOf(AttachmentTimestamp)
                   + Marshal.SizeOf(AttachmentTimestampLower)
                   + Marshal.SizeOf(AttachmentTimestampUpper)
                   + NonceBuffer.Length
                   + HashBuffer.Length);
        }

        private long _milestoneIndex = -1;
        /// <summary>
        /// Decodes a long value from <see cref="ObsoleteTag"/>
        /// </summary>
        /// <returns>The <see cref="ObsoleteTag"/> as a long</returns>
        public long GetMilestoneIndex()
        {
            if (_milestoneIndex != -1)
                return _milestoneIndex;

            var tritBuffer = new sbyte[15];
            Entangled<byte[]>.Default.Ternary.GetTritsFromFlexTrits((sbyte[]) (Array) ObsoleteTagBuffer.AsArray(), 0, tritBuffer, 15);

            try
            {
                _milestoneIndex = Entangled<byte[]>.Default.Ternary.GetLongFromFlexTrits(tritBuffer, 0, 15);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to parse milestone index from ObsoleteTag = `{AsTrytes(ObsoleteTagBuffer, IoTransaction.NUM_TRITS_OBSOLETE_TAG, IoTransaction.NUM_TRYTES_OBSOLETE_TAG)}' [{AsKeyString(HashBuffer)}]:");
                _milestoneIndex = 0;
            }

            return _milestoneIndex;
        }

        public long GetAttachmentTime()
        {
            return AttachmentTimestamp > 0 ? AttachmentTimestamp : Timestamp;
        }

        public string GetKey()
        {
            return Key;
        }
    }
}
