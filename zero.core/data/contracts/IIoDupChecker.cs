using System.Threading.Tasks;

namespace zero.core.data.contracts
{
    public interface IIoDupChecker
    {
        /// <summary>
        /// Is the dupchecker currently connected
        /// </summary>
        bool IsConnected { get; }

        Task<bool> IsDuplicate(string key);
    }
}
