using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.abstraction;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;

namespace zero.core.data.contracts
{
    public interface IIoData
    {
        bool IsConnected { get; }
        Task<RowSet> Put(IoInteropTransactionModel interopTransaction, object batch = null);
        Task<IoInteropTransactionModel> Get(string key);
        Task<RowSet> ExecuteAsync(object batch);
    }
}
