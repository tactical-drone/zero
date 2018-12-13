using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model.interop;

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
