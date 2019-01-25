using System;
using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model.interop;

namespace zero.core.data.contracts
{
    /// <summary>
    /// The data source interface
    /// </summary>
    /// <typeparam name="TBlob">The type of the blob field, string or byte array</typeparam>
    public interface IIoDataSource<TResult> 
    {
        /// <summary>
        /// Is the source currently connected
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Puts data to be stored
        /// </summary>
        /// <param name="transaction">The transaction to be stored</param>
        /// <param name="batch">A batch handler</param>
        /// <returns></returns>
        Task<TResult> Put<TBlob>(IIoTransactionModel<TBlob> transaction, object batch = null);            
        /// <summary>
        /// Get a transaction from storage
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        Task<IIoTransactionModel<TBlob>> Get<TBlob>(ReadOnlyMemory<byte> key);
        /// <summary>
        /// Execute a batch
        /// </summary>
        /// <param name="batch">The batch handler</param>
        /// <returns>Some result</returns>
        Task<TResult> ExecuteAsync(object batch);
    }
}
