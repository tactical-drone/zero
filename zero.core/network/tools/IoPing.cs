using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net;
using System.Text;
using System.Net.Sockets;
using System.Runtime.Serialization.Json;
using System.Threading.Tasks;
using zero.core.misc;
using zero.core.network.ip;

namespace zero.core.network.tools
{
    /// <summary>
    /// General tools for IP
    /// </summary>
    public class IoPing
    {
        /// <summary>
        /// Return default gateway
        /// </summary>
        public static IPAddress DefaultGateway => Trace().address;

        /// <summary>
        /// Return nat ip
        /// </summary>
        public static IPAddress Nat => Trace(hop: 2).address;

        /// <summary>
        /// Return nat ip
        /// </summary>
        public static long Latency => Probe(hop: 2).rtt;


        public static IPAddress Pub => GetInternetAddress();

        /// <summary>
        /// Return local lan ip
        /// </summary>
        public static IPAddress Lan => GetLocalIpAddress();

        /// <summary>
        /// Trace an IP at a certain <see cref="hop"/>
        /// </summary>
        /// <param name="dest">The destination ip</param>
        /// <param name="hop">Maximum hops to travel</param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static (IPAddress address,long ttl) Trace(IPAddress dest = null, int hop = 1)
        {
            dest ??= IPAddress.Parse("8.8.8.8");

            PingReply reply;
            using var ping = new Ping();
            var options = new PingOptions(hop, true);
            try
            {
                reply = ping.Send(dest, 550, Array.Empty<byte>(), options);
            }
            catch (PingException e)
            {
                throw new Exception($"Ip not available - {e.Message}");
            }

            if (reply != null && reply.Status != IPStatus.TtlExpired)
            {
                throw new Exception($"Trace not available - {dest}; hop = {hop}; status = {reply.Status}); in {reply.RoundtripTime}ms");
            }
            return (reply?.Address, reply?.RoundtripTime??0);
        }

        public static (IPAddress address, long rtt) Probe(IPAddress dest = null, int hop = 64)
        {
            dest ??= IPAddress.Parse("8.8.8.8");

            PingReply reply;
            using var ping = new Ping();
            var options = new PingOptions(hop, true);
            try
            {
                reply = ping.Send(dest, 550, Array.Empty<byte>(), options);
            }
            catch (PingException e)
            {
                throw new Exception($"Ip not available - {e.Message}");
            }

            if (reply != null && reply.Status != IPStatus.Success)
            {
                throw new Exception($"Trace not available - {dest}; hop = {hop}; status = {reply.Status}); in {reply.RoundtripTime}ms");
            }
            return (reply?.Address, reply?.RoundtripTime ?? 0);
        }

        private static IPAddress GetLocalIpAddress()
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Bind(new IPEndPoint(IPAddress.Any, 0));
            socket.Connect("127.0.0.1", 0);
            return ((IPEndPoint)socket.LocalEndPoint).Address;
        }

        private static IPAddress GetInternetAddress()
        {
            var buffer = new byte[4096];

            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Blocking = true;
            socket.ReceiveTimeout = 1000;
            socket.Bind(new IPEndPoint(IPAddress.Any, 0));
            socket.Connect("checkip.dyndns.org", 80);

            try
            {
                var sent = socket.Send(Encoding.ASCII.GetBytes("GET / HTTP/1.1\r\nHost: checkip.dyndns.org\r\n\r\n"));
                if (sent > 0)
                {
                    var read = socket.Receive(buffer);
                    if (read > 0)
                    {
                        var response = Encoding.ASCII.GetString(buffer[..read]);
                        var chunks = response.Split(": ")[^1].Split("<")[0].Split(".");
                        return new IPAddress(new[] { byte.Parse(chunks[0]), byte.Parse(chunks[1]), byte.Parse(chunks[2]), byte.Parse(chunks[3]) });
                    }
                }
            }
            catch
            {
                // ignored
            }

            return IPAddress.Any;
        }

        public static ConcurrentDictionary<string, NetworkInterface> Interfaces { get; protected set; }

        public static string Description()
        {
            Interfaces.Clear();
            var sb = new StringBuilder();
            foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                var ip = nic.GetIPProperties();
                if (!ip.UnicastAddresses.Any())
                    continue;

                Interfaces.TryAdd(nic.Id, nic);

                sb.Append($"{nic.Description} ip://{ip.UnicastAddresses.Last().Address}) mask://{ip.UnicastAddresses.Last().IPv4Mask}");

                try
                {
                    var hasGw = false;
                    foreach(var gw in nic.GetIPProperties().GatewayAddresses)
                    {
                        sb.AppendLine($"gw://{gw.Address}");
                        hasGw = true;
                    }

                    if (!hasGw)
                        sb.AppendLine();
                }
                catch
                {
                    // ignored
                }
            }

            return sb.ToString();
        }

        public static string Info()
        {
            var pub = IoPing.Pub;
            var nat = IoPing.Nat;
            return $"ip = {IoPing.Lan}, gateway = {IoPing.DefaultGateway}, nat = {nat}, pub = {pub} ({IoPing.Probe(nat).rtt}ms)";
        }

    }
}
