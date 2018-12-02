﻿using System;
using System.Threading.Tasks;
using NLog;
using zero.core.patterns.bushes.contracts;

namespace zero.core.network.ip
{
    /// <summary>
    /// The <see cref="IoNetClient{TJob}"/>'s TCP flavor
    /// </summary>
    /// <seealso cref="IoNetClient{TJob}" />
    public class IoTcpClient<TJob> : IoNetClient<TJob>
        where TJob : IIoWorker
        
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient{TJob}"/> class.
        /// </summary>
        public IoTcpClient()
        {
            _logger = LogManager.CreateNullLogger();
        }

        private readonly Logger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient"/> class.
        /// </summary>
        /// <param name="remote">The tcpclient to be wrapped</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoTcpClient(IoSocket remote, int readAhead) : base((IoNetSocket)remote, readAhead) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="IoTcpClient"/> class.
        /// </summary>
        /// <param name="address">The address associated with this network client</param>
        /// <param name="readAhead">The amount of socket reads the producer is allowed to lead the consumer</param>
        public IoTcpClient(IoNodeAddress address, int readAhead) : base(address, readAhead) { }

        /// <summary>
        /// Connects to a remote listener
        /// </summary>
        /// <returns>
        /// True if succeeded, false otherwise
        /// </returns>
        public override async Task<bool> ConnectAsync()
        {
            IoSocket = new IoTcpSocket(Spinners.Token);
            return await base.ConnectAsync();
        }        
    }
}
