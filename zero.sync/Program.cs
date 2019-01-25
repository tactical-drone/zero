﻿using System;
using System.Collections;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
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

            if (IoEntangled.Optimized)
            {
                var tangleNode = new IoNode<IoTangleMessage>(IoNodeAddress.Create("tcp://192.168.1.2:15600"), ioNetClient => new TanglePeer(ioNetClient), TanglePeer.TcpReadAhead);
                //var tangleNode = new IoNode(IoNodeAddress.Create("udp://192.168.1.2", 14600), ioNetClient=>new TanglePeer(ioNetClient));
#pragma warning disable 4014
                tangleNode.Start();
                tangleNode.SpawnConnectionAsync(IoNodeAddress.Create("tcp://unimatrix.uksouth.cloudapp.azure.com:15600"));
#pragma warning restore 4014
                //tangleNode.SpawnConnectionAsync(IoNodeAddress.Create("udp://unimatrix.uksouth.cloudapp.azure.com", 14600));


                Console.WriteLine("Press any key to shutdown");

                Console.ReadLine();
                tangleNode.Stop();
                Console.WriteLine("Shutting down");
                Console.ReadLine();
            }
            else
            {
                var tangleNode = new IoNode<IoTangleMessage>(IoNodeAddress.Create("tcp://192.168.1.2:15600"), ioNetClient => new TanglePeer(ioNetClient), TanglePeer.TcpReadAhead);
                //var tangleNode = new IoNode(IoNodeAddress.Create("udp://192.168.1.2", 14600), ioNetClient=>new TanglePeer(ioNetClient));
#pragma warning disable 4014
                tangleNode.Start();
                tangleNode.SpawnConnectionAsync(IoNodeAddress.Create("tcp://unimatrix.uksouth.cloudapp.azure.com:15600"));
#pragma warning restore 4014
                //tangleNode.SpawnConnectionAsync(IoNodeAddress.Create("udp://unimatrix.uksouth.cloudapp.azure.com", 14600));


                Console.WriteLine("Press any key to shutdown");

                Console.ReadLine();
                tangleNode.Stop();
                Console.WriteLine("Shutting down");
                Console.ReadLine();
            }            
        }
    }
}
