using System;
using System.Runtime.Loader;
using System.Threading.Tasks;
using NLog;
using zero.core.core;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.protocol;
using zero.interop.entangled;

namespace zero.sync
{
    class Program
    {
        static void Main(string[] args)
        {
            LogManager.LoadConfiguration("nlog.config");

            var listenerAddress = "tcp://192.168.1.2:15600";

            if (IoEntangled<string>.Optimized)
            {
                var tangleNode = new TangleNode<IoTangleMessage<byte[]>>(IoNodeAddress.Create(listenerAddress), ioNetClient => new TanglePeer<byte[]>(ioNetClient), TanglePeer<byte[]>.TcpReadAhead);
                
#pragma warning disable 4014
                var tangleNodeTask = tangleNode.StartAsync();

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Stop();
                Console.CancelKeyPress+= (sender, eventArgs) => tangleNode.Stop();                
                tangleNodeTask.Wait();                                                
            }
            else
            {
                var tangleNode = new TangleNode<IoTangleMessage<string>>(IoNodeAddress.Create(listenerAddress), ioNetClient => new TanglePeer<string>(ioNetClient), TanglePeer<string>.TcpReadAhead);
                
                var tangleNodeTask = tangleNode.StartAsync();
#pragma warning restore 4014

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Stop();
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Stop();
                tangleNodeTask.Wait();
            }            
        }
    }
}
