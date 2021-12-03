using System;
using System.Net;
using System.Net.Sockets;

namespace zero.core.feat.misc
{
    [Serializable]
    public class IoSocketAddress:SocketAddress
    {
        public IoSocketAddress(AddressFamily family) : base(family)
        {
            
        }

        public IoSocketAddress(AddressFamily family, int size) : base(family, size)
        {
        }
    }
}
