using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model.native;

namespace zero.core.data.native.contracts
{
    public interface IIoNativeData
    {
        bool IsConnected { get; }
        Task<RowSet> Put(IoNativeTransactionModel transaction, BatchStatement batch = null);
        Task<IoNativeTransactionModel> Get(string key);
        Task<RowSet> ExecuteAsync(BatchStatement batch);
    }
}
