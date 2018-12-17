using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.model.interop;

namespace zero.interop.entangled.common.model.native
{
    [Table("bundle")]
    public class IoNativeTransactionModel : IIoInteropTransactionModel<string>
    {                
        public string Address { get; set; }
        
        public string SignatureOrMessage { get; set; }
        
        public long Value { get; set; }
        
        public string ObsoleteTag { get; set; }
        
        public long Timestamp { get; set; }
        
        public long CurrentIndex { get; set; }
        
        public long LastIndex { get; set; }
        
        public string Bundle { get; set; }

        public string Trunk { get; set; }
        public string Branch { get; set; }
        
        public string Tag { get; set; }

        [Column(nameof(IoMarshalledTransaction.attachment_timestamp))]
        public long AttachmentTimestamp { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_lower))]
        public long AttachmentTimestampLower { get; set; }
        [Column(nameof(IoMarshalledTransaction.attachment_timestamp_upper))]
        public long AttachmentTimestampUpper { get; set; }
        public string Nonce { get; set; }
        
        public string Hash { get; set; }
        
        [Ignore]
        public long SnapshotIndex { get; set; }

        [Ignore]
        public bool Solid { get; set; }

        [Ignore]
        public sbyte Pow { get; set; }

        [Ignore]
        public sbyte FakePow { get; set; }

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

        [Ignore] //TODO config
        public string Body { get; set; }        

        public string AsTrytes(string field, int size)
        {
            return field;
        }

        public short Size { get; set; }
    }
}
