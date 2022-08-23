using zero.core.feat.api;
using zero.core.network.ip;

namespace zero.tangle.api.interfaces
{
    /// <summary>
    /// Node services interface
    /// </summary>    
    public interface IIoNodeController
    {
        /// <summary>
        /// Starts a new node listener at the specified address
        /// </summary>
        /// <param name="address">The address to listen at</param>
        /// <returns>true on success, false otherwise</returns>
        IoApiReturn Post(IoNodeAddress address);
    }
}