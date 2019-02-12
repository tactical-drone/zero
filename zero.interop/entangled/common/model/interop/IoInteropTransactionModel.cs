using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using NLog;
using zero.core.models;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;
using zero.interop.utils;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// Implements a interop transaction model
    /// </summary>
    public class IoInteropTransactionModel : IIoTransactionModel<byte[]> //TODO base this
    {
        public IoInteropTransactionModel()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private Logger _logger;
        public ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }
        public byte[] SignatureOrMessage
        {
            get => SignatureOrMessageBuffer.AsArray();
            set => SignatureOrMessageBuffer = new ReadOnlyMemory<byte>(value);
        }

        public ReadOnlyMemory<byte> AddressBuffer { get; set; }
        public byte[] Address
        {
            get => AddressBuffer.AsArray();
            set => AddressBuffer = new ReadOnlyMemory<byte>(value);
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
            set => BundleBuffer = new ReadOnlyMemory<byte>(value);
        }

        public ReadOnlyMemory<byte> TrunkBuffer { get; set; }
        public byte[] Trunk
        {
            get => TrunkBuffer.AsArray();
            set => TrunkBuffer = new ReadOnlyMemory<byte>(value);
        }

        public ReadOnlyMemory<byte> BranchBuffer { get; set; }
        public byte[] Branch
        {
            get => BranchBuffer.AsArray();
            set => BranchBuffer = new ReadOnlyMemory<byte>(value);
        }

        public ReadOnlyMemory<byte> TagBuffer { get; set; }
        public byte[] Tag
        {
            get => TagBuffer.AsArray();            
            set => TagBuffer = new ReadOnlyMemory<byte>(value);
        }

        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> NonceBuffer { get; set; }
        public byte[] Nonce
        {
            get => NonceBuffer.AsArray();
            set => NonceBuffer = new ReadOnlyMemory<byte>(value);
        }

        public ReadOnlyMemory<byte> HashBuffer { get; set; }
        public byte[] Hash
        {
            get => HashBuffer.AsArray();
            set => HashBuffer = new ReadOnlyMemory<byte>(value);
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

        public ReadOnlyMemory<byte> Blob { get; set; }

        public string Key { get; set; }

        public string AsTrytes(ReadOnlyMemory<byte> field, int fixedLenTritsToConvert = 0)
        {
            if (field.Length == 0)
                return string.Empty;

            var tritsToConvert = IoFlexTrit.NUM_TRITS_PER_FLEX_TRIT * field.Length;
            tritsToConvert += -tritsToConvert % Codec.TritsPerTryte;
            tritsToConvert = fixedLenTritsToConvert == 0 ? tritsToConvert : fixedLenTritsToConvert;

            var trytesToConvert = tritsToConvert / Codec.TritsPerTryte;

            var trytes = new sbyte[trytesToConvert];
            //Console.Write($"[{trytes.Length},{tritsToConvert}]");
            IoEntangled<byte[]>.Default.Ternary.GetTrytesFromFlexTrits(trytes, trytes.Length, (sbyte[])(Array)field.ToArray(), 0, tritsToConvert, tritsToConvert);            
            return Encoding.ASCII.GetString((byte[])(Array)trytes);
        }

        public ReadOnlyMemory<byte> AsBlob()
        {
            return Blob;
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
            IoEntangled<byte[]>.Default.Ternary.GetTritsFromFlexTrits((sbyte[]) (Array) ObsoleteTagBuffer.AsArray(), 0, tritBuffer, 15);

            try
            {
                _milestoneIndex = IoEntangled<byte[]>.Default.Ternary.GetLongFromFlexTrits(tritBuffer, 0, 15);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"Unable to parse milestone index from ObsoleteTag = `{AsTrytes(ObsoleteTagBuffer)}' [{AsTrytes(HashBuffer)}]:");
                _milestoneIndex = 0;
            }

            return _milestoneIndex;
        }

        public string GetKey()
        {
            return Key;
        }
    }
}
