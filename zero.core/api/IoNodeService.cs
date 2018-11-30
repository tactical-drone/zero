using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using NLog;
using NLog.Targets;
using Tangle.Net.Entity;
using zero.core.api.interfaces;
using zero.core.api.models;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.protocol;

namespace zero.core.api
{
    /// <summary>
    /// Node services
    /// </summary>
    /// <seealso cref="Microsoft.AspNetCore.Mvc.Controller" />
    /// <seealso cref="zero.core.api.interfaces.IIoNodeService" />
    [EnableCors("ApiCorsPolicy")]
    [ApiController]
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("/api/node")]    
    public class IoNodeService : Controller, IIoNodeService
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoNodeService"/> class.
        /// </summary>
        public IoNodeService()
        {
            _logger = LogManager.GetCurrentClassLogger();

            _apiLogger = LogManager.Configuration.FindTargetByName<MemoryTarget>("apiLogger"); 
            
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The nlog API logger hooks
        /// </summary>
        private readonly MemoryTarget _apiLogger;

        /// <summary>
        /// The nodes managed by this service
        /// </summary>
        private static readonly ConcurrentDictionary<int, IoNode<IoTangleMessage>> _nodes = new ConcurrentDictionary<int, IoNode<IoTangleMessage>>();
        /// <summary>
        /// Posts the specified address.
        /// </summary>
        /// <param name="address">The listening address to start a node on</param>
        /// <returns>true on success, false otherwise</returns>
        [HttpPost]
       public IoApiReturn Post(IoNodeAddress address)
        {
            if( !address.IsValid )
                return IoApiReturn.Result(false,address.ValidationErrorString);

            if (!_nodes.TryAdd(address.Port, new IoNode<IoTangleMessage>(address, ioNetClient => new TanglePeer(ioNetClient))))
            {
                var errStr = $"Cannot create node `${address.UrlAndPort}', a node with that id already exists";
                _logger.Warn(errStr);
                return IoApiReturn.Result(true, errStr);
            }

            var dbgStr = $"Added listener at `{address.UrlAndPort}'";
            
            _nodes[address.Port].Start();

            _logger.Debug(dbgStr);
            return IoApiReturn.Result(true, dbgStr, address.Port);
        }

        [Route("/api/node/logs")]
        [HttpGet]
        public IoApiReturn Logs()
        {            
            var data = _apiLogger.Logs.Select(l => new IoLogEntry(l)).ToList();
            var retval = IoApiReturn.Result(true,$"{data.Count} {nameof(Logs)} returned",data);
            _apiLogger.Logs.Clear();
            return retval;            
        }

        [Route("/api/node/stream/{id}")]
        [HttpGet]
        public IoApiReturn TransactionStreamQuery([FromRoute]int id, [FromQuery] string tagQuery)
        {
            if(!_nodes.ContainsKey(id))
                return IoApiReturn.Result(false, $"Could not find listener with id=`{id}'");
            var transactions = new List<Transaction>();

            int count = 0;
            long outstanding = 0;
            long freeBufferSpace = 0;
#pragma warning disable 4014
            _nodes.SelectMany(n => n.Value.Neighbors).Where(n => n.Key == id).Select(n => n.Value).ToList()
                .ForEach(async n =>
#pragma warning restore 4014
                {
                    var hub = n.PrimaryProducer.GetForwardProducer<IoTangleTransaction>();

                    if (hub != null)
                    {

                        while (Interlocked.Read(ref hub.JobMetaHeap.ReferenceCount) > 0 && count < 100)
                        {
                            await hub.ConsumeAsync(message =>
                            {
                                if(message == null)
                                    return;

                                var msg = ((IoTangleTransaction) message);
                                if (msg.Transaction.Tag.Value.IndexOf(tagQuery, 0, StringComparison.CurrentCultureIgnoreCase) != -1)
                                    transactions.Add(msg.Transaction);
                            }, sleepOnProducerLag:false);
                            count++;
                        }

                        outstanding = Interlocked.Read(ref hub.JobMetaHeap.ReferenceCount);
                        freeBufferSpace = hub.JobMetaHeap.FreeCapacity();
                    }
                    else
                        _logger.Warn($"Waiting for multicast producer `{n.PrimaryProducer.Description}' to initialize...");
                });

            //TODO remove diagnostic output
            return IoApiReturn.Result(true, $"Found `{transactions.Count}' transactions, scanned= `{count}', backlog= `{outstanding}', free= `{freeBufferSpace}'", transactions);            
        }

        [Route("/api/node/stopListener/{id}")]
        [HttpGet]
        public IoApiReturn StopListener([FromRoute] int id)
        {
            if(!_nodes.ContainsKey(id))
                return IoApiReturn.Result(false, $"Neighbor with listener port `{id}' does not exist");
            
            _nodes[id].Stop();
            _nodes.TryRemove(id, out _);

            return IoApiReturn.Result(true, $"Successfully stopped neighbor `{id}'");
        }
    }
}
