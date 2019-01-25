using System;
using System.Linq;
using System.Text;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// Implements a interop transaction model
    /// </summary>
    public class IoInteropTransactionModel : IIoTransactionModel //TODO base this
    {
        public ReadOnlyMemory<byte> SignatureOrMessage { get; set; }
        public ReadOnlyMemory<byte> Address { get; set; }
        public long Value { get; set; }
        public ReadOnlyMemory<byte> ObsoleteTag { get; set; }
        public long Timestamp { get; set; }
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
        public ReadOnlyMemory<byte> Bundle { get; set; }
        public ReadOnlyMemory<byte> Trunk { get; set; }
        public ReadOnlyMemory<byte> Branch { get; set; }
        public ReadOnlyMemory<byte> Tag { get; set; }
        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> Nonce { get; set; }
        public ReadOnlyMemory<byte> Hash { get; set; }
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
            IoEntangled.Default.Ternary.GetTrytesFromFlexTrits(trytes, trytes.Length, (sbyte[])(Array)field.ToArray(), 0, tritsToConvert, tritsToConvert);
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
