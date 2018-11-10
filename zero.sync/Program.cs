using System;
using NLog;
using zero.core.core;
using zero.core.protocol;

namespace zero.sync
{
    class Program
    {
        static void Main(string[] args)
        {
            LogManager.LoadConfiguration("nlog.config");

            var tangleNode = new IoNode<TanglePeer>(ioNetClient=>new TanglePeer(ioNetClient));

            #pragma warning disable 4014
            tangleNode.Start();
            #pragma warning restore 4014
            
            Console.WriteLine("Press any key to shutdown");
            
            Console.ReadLine();
            tangleNode.Stop();
            Console.WriteLine("Shutting down");
            Console.ReadLine();
        }
    }
}
