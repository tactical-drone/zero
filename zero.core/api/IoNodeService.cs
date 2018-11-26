using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using NLog;
using zero.core.api.interfaces;
using zero.core.api.models;
using zero.core.core;
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
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The nodes managed by this service
        /// </summary>
        private readonly ConcurrentDictionary<string, IoNode> _nodes = new ConcurrentDictionary<string, IoNode>();

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

            if (!_nodes.TryAdd(address.UrlAndPort, new IoNode(address, ioNetClient => new TanglePeer(ioNetClient))))
            {
                var errStr = $"Cannot create node `${address.UrlAndPort}', a node with that id already exists";
                _logger.Warn(errStr);
                return IoApiReturn.Result(true, errStr);
            }

            var dbgStr = $"Added node id = `{address.UrlAndPort}'";
            
            _nodes[address.UrlAndPort].Start();

            _logger.Debug(dbgStr);
            return IoApiReturn.Result(true, dbgStr);
        }

        [Route("/api/node/logs")]
        [HttpGet]
        public IoApiReturn Logs()
        {
            return IoApiReturn.Result(true, "logs", new[] {new IoLogEntry{logMsg="Entry 1"}, new IoLogEntry { logMsg = "Entry 2" }, new IoLogEntry { logMsg = "Entry 3" }});
        }

    }
}
