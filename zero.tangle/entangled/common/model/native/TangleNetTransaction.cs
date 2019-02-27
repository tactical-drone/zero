using System;
using System.Runtime.InteropServices;
using System.Text;
using Cassandra.Mapping.Attributes;
using NLog;
using zero.core.models;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.trinary;
using zero.tangle.models;

namespace zero.tangle.entangled.common.model.native
{
    /// <summary>
    /// Mocks a <see cref="IIoTransactionModelInterface"/> model when not using interop decoders
    /// </summary>
    public class TangleNetTransaction : IIoTransactionModel<string>
    {
        public TangleNetTransaction()
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        private Logger _logger;

        public string SignatureOrMessage
        {            
            get => Encoding.UTF8.GetString(SignatureOrMessageBuffer.Span);
            set => SignatureOrMessageBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawSignatureOrMessage { get; set; }

        public ReadOnlyMemory<byte> AddressBuffer { get; set; }
        public string Address
        {
            get => Encoding.UTF8.GetString(AddressBuffer.Span);
            set => AddressBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawAddress { get; set; }

        public ReadOnlyMemory<byte> SignatureOrMessageBuffer { get; set; }
        
        public long Value { get; set; }
        
        public ReadOnlyMemory<byte> ObsoleteTagBuffer { get; set; }
        public string ObsoleteTag
        {
            get => Encoding.UTF8.GetString(ObsoleteTagBuffer.Span);
            set => ObsoleteTagBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawObsoleteTag { get; set; }

        public long Timestamp { get; set; }
        
        public long CurrentIndex { get; set; }
        
        public long LastIndex { get; set; }
        
        public ReadOnlyMemory<byte> BundleBuffer { get; set; }
        public string Bundle
        {
            get => Encoding.UTF8.GetString(BundleBuffer.Span);
            set => BundleBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawBundle { get; set; }

        public ReadOnlyMemory<byte> TrunkBuffer { get; set; }
        public string Trunk
        {
            get => Encoding.UTF8.GetString(TrunkBuffer.Span);
            set => TrunkBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawTrunk { get; set; }

        public ReadOnlyMemory<byte> BranchBuffer { get; set; }
        public string Branch
        {
            get => Encoding.UTF8.GetString(BranchBuffer.Span);
            set => BranchBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawBranch { get; set; }

        public ReadOnlyMemory<byte> TagBuffer { get; set; }
        public string Tag
        {
            get => Encoding.UTF8.GetString(TagBuffer.Span);
            set => TagBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawTag { get; set; }

        [Column(nameof(IoMarshalledTransaction.attachments.attachment_timestamp))]
        public long AttachmentTimestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachments.attachment_timestamp_lower))]
        public long AttachmentTimestampLower { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachments.attachment_timestamp_upper))]
        public long AttachmentTimestampUpper { get; set; }
        public ReadOnlyMemory<byte> NonceBuffer { get; set; }
        public string Nonce
        {
            get => Encoding.UTF8.GetString(NonceBuffer.Span);
            set => NonceBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawNonce { get; set; }

        public ReadOnlyMemory<byte> HashBuffer { get; set; }
        public string Hash
        {
            get => Encoding.UTF8.GetString(HashBuffer.Span);
            set => HashBuffer = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(value));
        }

        public string RawHash { get; set; }

        [Ignore]
        public string Snapshot { get; set; }

        [Ignore]
        public long MilestoneIndexEstimate { get; set; }

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

        public bool IsMilestoneTransaction { get; set; }
        public int ConfirmationTime { get; set; }
        public IIoTransactionModel<string> MilestoneEstimateTransaction { get; set; }

        public string AsTrytes(ReadOnlyMemory<byte> field, int maxFlexTritsToConvert = IoFlexTrit.FLEX_TRIT_SIZE_243, int tryteLen = IoTransaction.NUM_TRYTES_HASH)
        {
            return Encoding.UTF8.GetString(field.Span).PadRight(tryteLen, '9');
        }

        public string AsKeyString(ReadOnlyMemory<byte> field, int maxFlexTritsToConvert = IoFlexTrit.FLEX_TRIT_SIZE_243, int tryteLen = IoTransaction.NUM_TRYTES_HASH)
        {
            return AsTrytes(field, maxFlexTritsToConvert, tryteLen);
        }

        public string Trimmed(string field, byte _)
        {
            return field.TrimEnd('9');
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
        public long GetMilestoneIndex()
        {
            if (_milestoneIndex != -1)
                return _milestoneIndex;

            try
            {                
                _milestoneIndex = Entangled<string>.Default.Ternary.GetLongFromFlexTrits((sbyte[])(Array)Encoding.UTF8.GetBytes(ObsoleteTag), 0, 15);                
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Unable to parse milestone index from ObsoleteTag = `{ObsoleteTag}' [{Hash}]:");
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
            return Hash;
        }

        public short Size { get; set; }        
    }
}
