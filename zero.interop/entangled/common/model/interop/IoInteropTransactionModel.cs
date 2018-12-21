using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Cassandra;
using Cassandra.Mapping.Attributes;
using zero.interop.entangled.common.trinary;
using zero.interop.entangled.mock;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model.interop
{

    [System.ComponentModel.DataAnnotations.Schema.Table("bundle")]    
    public class IoInteropTransactionModel : IIoInteropTransactionModel<byte[]> //TODO base this
    {        
        public byte[] SignatureOrMessage { get; set; }
        public byte[] Address { get; set; }
        public long Value { get; set; }
        public byte[] ObsoleteTag { get; set; }       
        public long Timestamp { get; set; }
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
        public byte[] Bundle { get; set; }
        public byte[] Trunk { get; set; }
        public byte[] Branch { get; set; }
        public byte[] Tag { get; set; }     
        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public byte[] Nonce { get; set; }
        public byte[] Hash { get; set; }   
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

        public string AsTrytes(byte[] field)
        {            
            if (field == null || field.Length == 0)
                return string.Empty;

            var tritsToConvert = IoFlexTrit.NUM_TRITS_PER_FLEX_TRIT * field.Length;
            tritsToConvert +=-tritsToConvert % Codec.TritsPerTryte;
            var trytesToConvert = tritsToConvert / Codec.TritsPerTryte;

            var trytes = new sbyte[trytesToConvert];
            Console.Write($"[{trytes.Length},{tritsToConvert}]");
            IoEntangled<byte[]>.Default.Ternary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)field, 0, tritsToConvert, tritsToConvert);
            return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray()); //TODO fix cast
        }
    }   
}
