using Newtonsoft.Json.Linq;
using zero.core.network.ip;

namespace zero.core.api.interfaces
{
    /// <summary>
    /// Node services interface
    /// </summary>
    public interface IIoNodeService
    {
        /// <summary>
        /// Starts a new node listener at the specified address
        /// </summary>
        /// <param name="address">The address to listen at</param>
        /// <returns>true on success, false otherwise</returns>
        IoApiReturn Post(JObject address);
    }
}