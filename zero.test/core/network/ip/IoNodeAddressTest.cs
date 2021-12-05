using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using zero.core.network.ip;

namespace zero.test.core.network.ip
{
    public class IoNodeAddressTest
    {
        [Fact]
        void Equality()
        {
            var address1 = new IoNodeAddress("udp://127.0.0.1:1234");
            var address2 = new IoNodeAddress("udp://127.0.0.1:1234");

            Assert.Equal(address1, address2);
            Assert.Equal(address1.IpEndPoint, address2.IpEndPoint);
            Assert.True(Equals(address1.IpEndPoint, address2.IpEndPoint));

            Assert.Equal("127.0.0.1", address1.Ip);
            Assert.Equal(1234, address1.Port);
            Assert.Equal("127.0.0.1:1234", address1.IpPort);
            Assert.Equal("127.0.0.1:1234", address1.EndpointIpPort);
            Assert.Equal("udp://", address1.ProtocolDesc);
            Assert.True(address1.Validated);
        }
    }
}
