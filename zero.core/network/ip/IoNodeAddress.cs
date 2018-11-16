using System;
using System.Collections.Generic;
using System.Text;

namespace zero.core.network.ip
{
    /// <summary>
    /// Used to store address information of remote nodes
    /// </summary>
    public class IoNodeAddress
    {
        /// <summary>
        /// Constructs a new node address
        /// </summary>
        /// <param name="url">The node url in the form tcp:// or udp://</param>
        /// <param name="port">The node listening port</param>
        public IoNodeAddress(string url, int port)
        {
            Url = url;
            Port = port;
        }

        /// <summary>
        /// The address url of the node
        /// </summary>
        public string Url;

        /// <summary>
        /// The listening port of the remote node
        /// </summary>
        public int Port;

        /// <summary>
        /// Creates a new node address descriptor
        /// </summary>
        /// <param name="url">The node url in the form tcp:// or udp://</param>
        /// <param name="port">The node listening port</param>
        /// <returns></returns>
        public static IoNodeAddress Create(string url, int port)
        {
            return new IoNodeAddress(url, port);
        }
    }
}
