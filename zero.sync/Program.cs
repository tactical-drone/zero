using System;
using System.Threading.Tasks;
using zero.core.network;
using zero.core.patterns;
using zero.core.patterns.misc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using zero.core.core;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using LogLevel = NLog.LogLevel;

namespace zero.sync
{
    class Program
    {
        static void Main(string[] args)
        {
            LogManager.LoadConfiguration("nlog.config");

            var node = new Node();

            #pragma warning disable 4014
            node.Start();
            #pragma warning restore 4014
            
            Console.WriteLine("Press any key to shutdown");
            
            Console.ReadLine();
            node.Stop();
            Console.WriteLine("Shutting down");
            Console.ReadLine();
        }
    }
}
