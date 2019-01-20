using Tangle.Net.Cryptography;
using Tangle.Net.Cryptography.Curl;
using Tangle.Net.Entity;
using Tangle.Net.Utils;

namespace zero.interop.entangled.common.model.native
{
    /// <summary>
    /// A transaction model used for mocking interop functionality
    /// </summary>
    public class IoMockTransaction: Transaction
    {
        public long SnapshotIndex = 0;
        public bool Solid = false;

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
            };            
            return tx;
        }
    }
}
