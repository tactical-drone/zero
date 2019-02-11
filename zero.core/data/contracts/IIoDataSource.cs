﻿using System;
using System.Threading.Tasks;
using zero.core.models;

namespace zero.core.data.contracts
{
    /// <summary>
    /// The data source interface
    /// </summary>    
    /// <typeparam name="TResult">The result type from db transactions</typeparam>
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
        /// <param name="userData">A batch handler</param>
        /// <returns></returns>
        Task<TResult> PutAsync<TBlob>(IIoTransactionModel<TBlob> transaction, object userData = null);            

        /// <summary>
        /// Get a transaction from storage
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        Task<IIoTransactionModel<TBlobF>> GetAsync<TBlobF>(TBlobF key);

        /// <summary>
        /// Checks if a transaction has been loaded.
        /// </summary>
        /// <typeparam name="TBlob"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        Task<bool> TransactionExistsAsync<TBlob>(TBlob key);
        
        /// <summary>
        /// Execute a batch
        /// </summary>
        /// <param name="usedData">The batch handler</param>
        /// <returns>Some result</returns>
        Task<TResult> ExecuteAsync(object usedData);
    }
}
