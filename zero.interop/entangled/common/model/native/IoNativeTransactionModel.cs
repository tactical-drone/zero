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
    public class IoNativeTransactionModel : IIoTransactionModel<string>
    {
        public string SignatureOrMessage
        {            
            get => Encoding.UTF8.GetString(SignatureOrMessageBuffer.Span);
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> AddressBuffer { get; set; }
        public string Address
        {
            get => Encoding.UTF8.GetString(AddressBuffer.Span);
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }
        
        public long Value { get; set; }
        
        public ReadOnlyMemory<byte> ObsoleteTagBuffer { get; set; }
        public string ObsoleteTag
        {
            get => Encoding.UTF8.GetString(ObsoleteTagBuffer.Span);
            set => throw new NotImplementedException();
        }

        public long Timestamp { get; set; }
        
        public long CurrentIndex { get; set; }
        
        public long LastIndex { get; set; }
        
        public ReadOnlyMemory<byte> BundleBuffer { get; set; }
        public string Bundle
        {
            get => Encoding.UTF8.GetString(BundleBuffer.Span);
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> TrunkBuffer { get; set; }
        public string Trunk
        {
            get => Encoding.UTF8.GetString(TrunkBuffer.Span);
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> BranchBuffer { get; set; }
        public string Branch
        {
            get => Encoding.UTF8.GetString(BranchBuffer.Span);
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> TagBuffer { get; set; }
        public string Tag
        {
            get => Encoding.UTF8.GetString(TagBuffer.Span);
            set => throw new NotImplementedException();
        }

        [Column(nameof(IoMarshalledTransaction.attachment_timestamp))]
        public long AttachmentTimestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_lower))]
        public long AttachmentTimestampLower { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_upper))]
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> NonceBuffer { get; set; }
        public string Nonce
        {
            get => Encoding.UTF8.GetString(NonceBuffer.Span);
            set => throw new NotImplementedException();
        }

        public ReadOnlyMemory<byte> HashBuffer { get; set; }
        public string Hash
        {
            get => Encoding.UTF8.GetString(HashBuffer.Span);
            set => throw new NotImplementedException();
        }

        [Ignore]
        public string Snapshot { get; set; }

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
            return Hash;
        }

        public short Size { get; set; }        
    }
}
