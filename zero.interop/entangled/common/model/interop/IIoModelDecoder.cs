namespace zero.interop.entangled.common.model.interop
{
    /// <summary>
    /// The data model decoder contract
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public interface IIoModelDecoder<TBlob> 
    {
        /// <summary>
        /// Deserialize ioterop model from flex trits
        /// </summary>
        /// <param name="flexTritBuffer">The flex trits</param>
        /// <param name="buffOffset">Offset into the buffer</param>
        /// <param name="tritBuffer">Some buffer space</param>
        /// <returns>The deserialized transaction</returns>
        IIoTransactionModel<TBlob> GetTransaction(sbyte[] flexTritBuffer, int buffOffset, sbyte[] tritBuffer = null);
    }
}
