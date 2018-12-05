using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.trinary;

namespace zero.interop.entangled.common.model.mock
{
    public class IoNativeTransactionModel : IIoInteropTransactionModel
    {
        public string Address { get; set; }
        public string Message { get; set; }
        public long Value { get; set; }
        public string ObsoleteTag { get; set; }
        public long Timestamp { get; set; }
        public long CurrentIndex { get; set; }
        public long LastIndex { get; set; }
        public string Bundle { get; set; }
        public string Trunk { get; set; }
        public string Branch { get; set; }
        public string Tag { get; set; }
        public long AttachmentTimestamp { get; set; }
        public long AttachmentTimestampLower { get; set; }
        public long AttachmentTimestampUpper { get; set; }
        public string Nonce { get; set; }
        public string Hash { get; set; }
        public long SnapshotIndex { get; set; }
        public bool Solid { get; set; }

        //public override string Address
        //{
        //    get
        //    {

        //    };
        //    set
        //    {

        //    }
        //}

        //public void SetAddress(string addressString)
        //{
        //    fixed (sbyte* addressStringPtr = (sbyte[])(Array)addressString.ToCharArray())
        //    {
        //        fixed (sbyte* addressPtr = IoTransactionModel.address)
        //        {
        //            IoTritTryte.trytes_to_trits(addressStringPtr, addressPtr, IoFlexTrit.FLEX_TRIT_SIZE_6561);
        //        }
        //    }
        //}

        //[DataMember]
        //public string Address
        //{
        //    get
        //    {
        //        byte[] trytes = new byte[model.IoTransactionModel.NUM_TRYTES_ADDRESS];
        //        fixed (sbyte* addressPtr = IoTransactionModel.address)
        //        {
        //            fixed (byte* trytesPtr = trytes)
        //                IoTritTryte.trits_to_trytes(addressPtr, (sbyte*)trytesPtr, IoFlexTrit.FLEX_TRIT_SIZE_243);
        //        }

        //        return Encoding.ASCII.GetString(trytes);
        //    }
        //}

        //public void SetTag(string tagString)
        //{
        //    for (int i = 0; i < tagString.Length; i++)
        //    {
        //        Tag[i] = (sbyte)tagString[i];
        //    }
        //}

        //public void SetObsoleteTag(string tagString)
        //{
        //    for (int i = 0; i < tagString.Length; i++)
        //    {
        //        obsolete_tag[i] = (sbyte)tagString[i];
        //    }
        //}

        //public void SetHash(string hashString)
        //{
        //    for (int i = 0; i < hashString.Length; i++)
        //    {
        //        Hash[i] = (sbyte)hashString[i];
        //    }
        //}

    }
}
