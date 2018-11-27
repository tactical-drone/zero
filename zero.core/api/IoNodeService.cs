using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore.Internal;
using Newtonsoft.Json;
using NLog;
using NLog.Config;
using NLog.Targets;
using zero.core.api.interfaces;
using zero.core.api.models;
using zero.core.conf;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes.contracts;
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

        private readonly MemoryTarget _apiLogger;

        /// <summary>
        /// The nodes managed by this service
        /// </summary>
        private readonly ConcurrentDictionary<int, IoNode<IoTangleMessage>> _nodes = new ConcurrentDictionary<int, IoNode<IoTangleMessage>>();
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

        [HttpGet("/api/node/stream/{id}{tagQuery:maxlength(27)?}")]
        public IoApiReturn TransactionStreamQuery(int id, [FromBody] string tagQuery)
        {
            if(!_nodes.ContainsKey(id))
                return IoApiReturn.Result(false, $"Cound not find listener with id=`{id}'");

            foreach (var keyValuePair in _nodes[id].Neighbors)
            {
                var neighbor = keyValuePair.Value;
            }
            var data = _apiLogger.Logs.Select(l => new IoLogEntry(l)).ToList();
            var retval = IoApiReturn.Result(true, $"{data.Count} {nameof(Logs)} returned", data);
            _apiLogger.Logs.Clear();
            return retval;
        }
    }
}
