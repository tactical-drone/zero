using System.Runtime.Serialization;
using Tangle.Net.Cryptography;
using Tangle.Net.Cryptography.Curl;
using Tangle.Net.Entity;
using Tangle.Net.Utils;

namespace zero.interop.entangled.common.model.native
{
    public class IoMockTransaction: Transaction
    {
        public ulong SnapshotIndex;
        public bool Solid;

        private bool _checkedPow = false;
        private sbyte _hasPow;        
        private TransactionTrytes _trytes;
        private Hash _computedHash = null;

        public sbyte Pow
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

        public string Color
        {
            get
            {
                if (Pow == 0)
                    return "color: red";
                return Pow < 0 ? "color: orange" : "color:green";
            }
        }

        private sbyte CheckPow()
        {
            _checkedPow = true;
            var hashTrits = new int[Constants.TritHashLength];
            var kerl = new Curl();
            kerl.Absorb(_trytes.ToTrits());
            kerl.Squeeze(hashTrits);
            
            _computedHash = new Hash(Converter.TritsToTrytes(hashTrits));
            _hasPow = 0;

            if (_computedHash.Value.Contains(Hash.Value.Substring(0, 10)) && _computedHash.Value
                    .Substring(_computedHash.Value.Length - 11, 6)
                    .Contains(Hash.Value.Substring(Hash.Value.Length - 11, 6)))
            {
                for (var i = _computedHash.Value.Length - 1; i > 0 && _computedHash.Value[i--] == '9'; _hasPow++){}
            }
            else
            {
                for (var i = _computedHash.Value.Length - 1; i > 0 && _computedHash.Value[i--] == '9'; _hasPow--) { }
            }

            return _hasPow;
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
            var attachmentTimestamp = trytes.GetChunk(2619, 9).ToTrits();
            var attachmentTimestampLower = trytes.GetChunk(2628, 9).ToTrits();
            var attachmentTimestampUpper = trytes.GetChunk(2637, 9).ToTrits();
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
                AttachmentTimestamp = Converter.ConvertTritsToBigInt(attachmentTimestamp, 0, attachmentTimestamp.Length).LongValue,
                AttachmentTimestampLowerBound = Converter.ConvertTritsToBigInt(attachmentTimestampLower, 0, attachmentTimestampLower.Length).LongValue,
                AttachmentTimestampUpperBound = Converter.ConvertTritsToBigInt(attachmentTimestampUpper, 0, attachmentTimestampUpper.Length).LongValue,
                _trytes = trytes
            };
            return tx;
        }
    }
}
