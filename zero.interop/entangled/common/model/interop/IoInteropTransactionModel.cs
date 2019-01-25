using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
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
        public ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }
        public byte[] SignatureOrMessage
        {
            get => SignatureOrMessageBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> AddressBuffer { get; set; }
        public byte[] Address
        {
            get => AddressBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public long Value { get; set; }
        public ReadOnlyMemory<byte> ObsoleteTagBuffer { get; set; }
        public byte[] ObsoleteTag
        {
            get => ObsoleteTagBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public long Timestamp { get; set; }
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
        public ReadOnlyMemory<byte> BundleBuffer { get; set; }
        public byte[] Bundle
        {
            get => BundleBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> TrunkBuffer { get; set; }
        public byte[] Trunk
        {
            get => TrunkBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> BranchBuffer { get; set; }
        public byte[] Branch
        {
            get => BranchBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> TagBuffer { get; set; }
        public byte[] Tag
        {
            get => TagBuffer.AsArray();
            //get => TagBuffer.Span;
            set => throw new NotImplementedException();
        }

        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> NonceBuffer { get; set; }
        public byte[] Nonce
        {
            get => NonceBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> HashBuffer { get; set; }
        public byte[] Hash
        {
            get => HashBuffer.AsArray();
            set => throw new NotImplementedException();
        }

        public long SnapshotIndex { get; set; }
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
            return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray()); //TODO fix cast
        }

        public ReadOnlyMemory<byte> AsBlob()
        {
            return Blob;
        }

        public string GetKey()
        {
            return Key;
        }
    }
}
