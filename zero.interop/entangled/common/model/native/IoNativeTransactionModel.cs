using System;
using System.Runtime.InteropServices;
using System.Text;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.native
{
    /// <summary>
    /// Mocks a <see cref="IIoTransactionModel{TBlob}"/> model when not using interop decoders
    /// </summary>
    public class IoNativeTransactionModel : IIoTransactionModel
    {                
        public ReadOnlyMemory<byte> Address { get; set; }
        
        public ReadOnlyMemory<byte> SignatureOrMessage { get; set; }
        
        public long Value { get; set; }
        
        public ReadOnlyMemory<byte> ObsoleteTag { get; set; }
        
        public long Timestamp { get; set; }
        
        public long CurrentIndex { get; set; }
        
        public long LastIndex { get; set; }
        
        public ReadOnlyMemory<byte> Bundle { get; set; }

        public ReadOnlyMemory<byte> Trunk { get; set; }
        public ReadOnlyMemory<byte> Branch { get; set; }
        
        public ReadOnlyMemory<byte> Tag { get; set; }

        [Column(nameof(IoMarshalledTransaction.attachment_timestamp))]
        public long AttachmentTimestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_lower))]
        public long AttachmentTimestampLower { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_upper))]
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> Nonce { get; set; }
        
        public ReadOnlyMemory<byte> Hash { get; set; }
        
        [Ignore]
        public long SnapshotIndex { get; set; }

        [Ignore]
        public bool Solid { get; set; }

        [Ignore]
        public sbyte Pow { get; set; }

        [Ignore]
        public sbyte ReqPow { get; set; }

        [Ignore]
        public string Color
        {
            get
            {
                if (Pow == 0)
                    return "color: red";
                return Pow < 0 ? "color: orange" : "color:green";
            }
        }

        [Ignore]
        public string Uri { get; set; }

        [Ignore]
        public ReadOnlyMemory<byte> Blob { get; set; }

        public string AsTrytes(ReadOnlyMemory<byte> field, int fixedLenTritsToConvert = 0)
        {
            return Encoding.UTF8.GetString(field.Span);
        }

        public ReadOnlyMemory<byte> AsBlob()
        {
            return Blob;
        }

        public string GetKey()
        {
            return Encoding.UTF8.GetString(Hash.Span);
        }

        public short Size { get; set; }
    }
}
