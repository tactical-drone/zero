using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Cassandra.Mapping.Attributes;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model.interop
{

    [System.ComponentModel.DataAnnotations.Schema.Table("bundle")]    
    public class IoInteropTransactionModel : IIoInteropTransactionModel
    {
        [IgnoreDataMember]
        [Ignore]
        public IoMarshalledTransaction Mapping { get; set; }

        public string Address
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_ADDRESS];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.address, 0, IoTransaction.NUM_TRITS_ADDRESS, IoTransaction.NUM_TRITS_ADDRESS);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set
            {

            }
        }

        public string SignatureOrMessage
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_SIGNATURE];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.signature_or_message, 0, IoTransaction.NUM_TRITS_SIGNATURE, IoTransaction.NUM_TRITS_SIGNATURE);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set
            {

            }
        }

        public long Value
        {
            get => Mapping.value;
            set { }
        }
    

        public string ObsoleteTag
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_OBSOLETE_TAG];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.obsolete_tag, 0, IoTransaction.NUM_TRITS_OBSOLETE_TAG, IoTransaction.NUM_TRITS_OBSOLETE_TAG);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public long Timestamp
        {
            get => Mapping.timestamp;
            set { }
        }
        public long CurrentIndex
        {
            get => Mapping.current_index;
            set { }
        }
        public long LastIndex
        {
            get => Mapping.last_index;
            set { }
        }
        public string Bundle
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_BUNDLE];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.bundle, 0, IoTransaction.NUM_TRITS_BUNDLE, IoTransaction.NUM_TRITS_BUNDLE);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Trunk
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_TRUNK];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.trunk, 0, IoTransaction.NUM_TRITS_TRUNK, IoTransaction.NUM_TRITS_TRUNK);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Branch
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_BRANCH];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.branch, 0, IoTransaction.NUM_TRITS_BRANCH, IoTransaction.NUM_TRITS_BRANCH);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Tag
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_TAG];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.tag, 0, IoTransaction.NUM_TRITS_TAG, IoTransaction.NUM_TRITS_TAG);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public long AttachmentTimestamp
        {
            get => Mapping.attachment_timestamp;
            set { }
        }
        public long AttachmentTimestampLower
        {
            get => Mapping.attachment_timestamp_lower;
            set { }
        }
        public long AttachmentTimestampUpper
        {
            get => Mapping.attachment_timestamp_upper;
            set { }
        }
        public string Nonce
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_NONCE];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.nonce, 0, IoTransaction.NUM_TRITS_NONCE, IoTransaction.NUM_TRITS_NONCE);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public string Hash
        {
            get
            {
                var trytes = new sbyte[IoTransaction.NUM_TRYTES_HASH];
                IoEntangled.Default.Trinary.GetFlexTrytes(trytes, trytes.Length, (sbyte[])(Array)Mapping.hash, 0, IoTransaction.NUM_TRITS_HASH, IoTransaction.NUM_TRITS_HASH);
                return Encoding.ASCII.GetString(trytes.Select(t => (byte)(t)).ToArray());
            }
            set { }
        }
        public long SnapshotIndex
        {
            get => Mapping.snapshot_index;
            set { }
        }
        public bool Solid
        {
            get => Mapping.solid;
            set { }
        }

        public int Pow { get; set; }
        public int FakePow { get; set; }

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
    }   
}
