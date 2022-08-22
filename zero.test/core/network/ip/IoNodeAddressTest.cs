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
            var address2 = IoNodeAddress.Create("udp://127.0.0.1:1234");
            var address3 = IoNodeAddress.CreateFromEndpoint("udp", address2.IpEndPoint);

            Assert.Equal(address1, address2);
            Assert.Equal(address2, address3);
            Assert.Equal(address1.IpEndPoint, address2.IpEndPoint);
            Assert.Equal(address2.IpEndPoint, address3.IpEndPoint);
            Assert.True(Equals(address1.IpEndPoint, address2.IpEndPoint));
            Assert.True(Equals(address2.IpEndPoint, address3.IpEndPoint));

            CheckDerivedTypes(address1);
            CheckDerivedTypes(address2);
            CheckDerivedTypes(address3);
        }

        private void CheckDerivedTypes(IoNodeAddress ioNodeAddress)
        {
            Assert.Equal("127.0.0.1", ioNodeAddress.Ip);
            Assert.Equal(1234, ioNodeAddress.Port);
            Assert.Equal("127.0.0.1:1234", ioNodeAddress.IpPort);
            Assert.Equal("127.0.0.1:1234", ioNodeAddress.EndpointIpPort);
            Assert.Equal("udp://", ioNodeAddress.ProtocolDesc);
            Assert.True(ioNodeAddress.Validated);
        }
    }
}
