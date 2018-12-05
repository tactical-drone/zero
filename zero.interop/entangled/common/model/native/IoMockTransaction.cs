using System.Runtime.Serialization;
using Tangle.Net.Cryptography;
using Tangle.Net.Cryptography.Curl;
using Tangle.Net.Entity;
using Tangle.Net.Utils;

namespace zero.interop.entangled.common.model.native
{
    public class IoMockTransaction: Transaction
    {
        //metadata
        public long SnapshotIndex;
        public bool Solid;

        private bool _checkedPow = false;
        private bool _hasPow;        
        private TransactionTrytes _trytes;
        private Hash _computedHash = null;
        private int _difficulty = -1;

        //[DataMember]
        //public int Difficulty
        //{
        //    get
        //    {
        //        if (_difficulty == -1)
        //        {
        //            for (var i = Hash.Value.Length - 1; ;i--)
        //            {
        //                if (Hash.Value[i] == '9')
        //                    _difficulty++;
        //                else
        //                    break;
        //            }
        //        }

        //        return _difficulty;
        //    }
        //    set => _difficulty = value;
        //}

        [DataMember]
        public bool HasPow
        {
            get
            {
                if (_checkedPow)
                    return _hasPow;
                else
                    return _hasPow = CheckPow();
            }
            set => _hasPow = value;
        }

        [DataMember]
        public string Color => HasPow ? "color: green" : "color: red";

        private bool CheckPow()
        {
            _checkedPow = true;
            var hashTrits = new int[Constants.TritHashLength];
            var kerl = new Curl();
            kerl.Absorb(_trytes.ToTrits());
            kerl.Squeeze(hashTrits);
            
            _computedHash = new Hash(Converter.TritsToTrytes(hashTrits));
            
            return _computedHash.Value.Contains(Hash.Value.Substring(0,10)) && _computedHash.Value.Substring(_computedHash.Value.Length-10, 7).Contains(Hash.Value.Substring(Hash.Value.Length - 10, 7));            
        }

        public new static IoMockTransaction FromTrytes(TransactionTrytes trytes, Hash hash = null)
        {
            if (hash == null)
            {
                var hashTrits = new int[Constants.TritHashLength];
                var kerl = new Curl();
                kerl.Absorb(trytes.ToTrits());
                kerl.Squeeze(hashTrits);

                hash = new Hash(Converter.TritsToTrytes(hashTrits));
            }

            var valueTrits = trytes.GetChunk(2268, 27).ToTrits();
            var attachementTimestamp = trytes.GetChunk(2619, 9).ToTrits();
            var attachementTimestampLower = trytes.GetChunk(2628, 9).ToTrits();
            var attachementTimestampUpper = trytes.GetChunk(2637, 9).ToTrits();
            var timestamp = trytes.GetChunk(2322, 9).ToTrits();
            var tx= new IoMockTransaction
            {
                Address = trytes.GetChunk<Address>(2187, Address.Length),
                Hash = hash,
                Fragment = trytes.GetChunk<Fragment>(0, 2187),
                Value = Converter.ConvertTritsToBigInt(valueTrits, 0, valueTrits.Length).LongValue,
                ObsoleteTag = trytes.GetChunk<Tag>(2295, Tag.Length),
                Timestamp = Converter.ConvertTritsToBigInt(timestamp, 0, 27).LongValue,
                CurrentIndex = Converter.TritsToInt(trytes.GetChunk(2331, 9).ToTrits()),
                LastIndex = Converter.TritsToInt(trytes.GetChunk(2340, 9).ToTrits()),
                BundleHash = trytes.GetChunk<Hash>(2349, Hash.Length),
                TrunkTransaction = trytes.GetChunk<Hash>(2430, Hash.Length),
                BranchTransaction = trytes.GetChunk<Hash>(2511, Hash.Length),
                Tag = trytes.GetChunk<Tag>(2592, Tag.Length),
                Nonce = trytes.GetChunk<Tag>(2646, Tag.Length),
                AttachmentTimestamp = Converter.ConvertTritsToBigInt(attachementTimestamp, 0, attachementTimestamp.Length).LongValue,
                AttachmentTimestampLowerBound = Converter.ConvertTritsToBigInt(attachementTimestampLower, 0, attachementTimestampLower.Length).LongValue,
                AttachmentTimestampUpperBound = Converter.ConvertTritsToBigInt(attachementTimestampUpper, 0, attachementTimestampUpper.Length).LongValue,
                _trytes = trytes
            };
            return tx;
        }
    }
}
