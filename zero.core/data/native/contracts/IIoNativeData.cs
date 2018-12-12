﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model.native;

namespace zero.core.data.native.contracts
{
    public interface IIoNativeData
    {
        Task<RowSet> Put(IoNativeTransactionModel transaction);
        Task<IoNativeTransactionModel> Get(string key);
    }
}
