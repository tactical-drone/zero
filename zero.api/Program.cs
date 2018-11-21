﻿using System.IO;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Web;

namespace zero.api
{
    public class Program
    {
        public static void Main(string[] args)
        {
            InitNlog();

            BuildWebHost(args).Run();
        }

        private static Logger _logger;

        private static void InitNlog()
        {
           _logger = NLogBuilder.ConfigureNLog("nlog.config").GetCurrentClassLogger();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((hostingContext, configApp) =>
                {
                    configApp.SetBasePath(Directory.GetCurrentDirectory());
                    configApp.AddJsonFile("appsettings.json", optional: true);
                    configApp.AddJsonFile($"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json",optional: true);
                    configApp.AddEnvironmentVariables();
                    configApp.AddCommandLine(args);
                    
                })                
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                })
                .UseNLog()
                .UseStartup<Startup>()
                .Build();
    }
}
