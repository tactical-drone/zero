using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model.native;

namespace zero.core.data.native.contracts
{
    public interface IIoNativeData
    {
        bool IsConnected { get; }
        Task<RowSet> Put(IoNativeTransactionModel transaction, object batch = null);
        Task<IoNativeTransactionModel> Get(string key);
        Task<RowSet> ExecuteAsync(object batch);
    }
}
