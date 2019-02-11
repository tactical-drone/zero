using System;
using NLog;
using zero.core.network.ip;
using zero.interop.entangled;
using zero.tangle;
using zero.tangle.models;

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
                var tangleNode = new TangleNode<IoTangleMessage<byte[]>, byte[]>(IoNodeAddress.Create(listenerAddress), (node, ioNetClient) =>  new TanglePeer<byte[]>((TangleNode<IoTangleMessage<byte[]>, byte[]>) node, ioNetClient), TanglePeer<byte[]>.TcpReadAhead);
                
#pragma warning disable 4014
                var tangleNodeTask = tangleNode.StartAsync();

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Stop();
                Console.CancelKeyPress+= (sender, eventArgs) => tangleNode.Stop();                
                tangleNodeTask.Wait();                                                
            }
            else
            {
                var tangleNode = new TangleNode<IoTangleMessage<string>, string>(IoNodeAddress.Create(listenerAddress), (node, ioNetClient) => new TanglePeer<string>((TangleNode<IoTangleMessage<string>,string>) node, ioNetClient), TanglePeer<string>.TcpReadAhead);
                
                var tangleNodeTask = tangleNode.StartAsync();
#pragma warning restore 4014

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => tangleNode.Stop();
                Console.CancelKeyPress += (sender, eventArgs) => tangleNode.Stop();
                tangleNodeTask.Wait();
            }            
        }
    }
}
