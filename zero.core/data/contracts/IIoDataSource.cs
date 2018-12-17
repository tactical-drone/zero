using System.Threading.Tasks;
using Cassandra;
using zero.interop.entangled.common.model.interop;

namespace zero.core.data.contracts
{
    public interface IIoDataSource<TBlob>
    {
        bool IsConnected { get; }
        Task<RowSet> Put(IIoInteropTransactionModel<TBlob> transaction, object batch = null);
        Task<IoInteropTransactionModel> Get(TBlob key);
        Task<RowSet> ExecuteAsync(object batch);
    }
}
