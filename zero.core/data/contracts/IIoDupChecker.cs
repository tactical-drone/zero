using System;
using System.Threading.Tasks;
using zero.core.patterns.misc;

namespace zero.core.data.contracts
{
    public interface IIoDupChecker :IIoNanite
    {
        /// <summary>
        /// True if the backend is operational
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// The time window in which this dupchecker can identify duplicates
        /// </summary>
        TimeSpan DupCheckWindow { get; }

        /// <summary>
        /// Checks the existence of a key in the store
        /// </summary>
        /// <param name="key">The key to be tested</param>
        /// <returns>True if the key exists, false otherwise</returns>
        Task<bool> KeyExistsAsync(string key);

        /// <summary>
        /// Deletes a key from the store
        /// </summary>
        /// <param name="key">The key to be deleted</param>
        /// <returns>True if the key was found and deleted</returns>
        Task<bool> DeleteKeyAsync(string key);
    }
}
