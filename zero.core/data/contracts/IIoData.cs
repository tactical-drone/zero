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
        Task<RowSet> Put(IoTransactionModel transaction);
        Task<IoTransactionModel> Get(string key);
    }
}
