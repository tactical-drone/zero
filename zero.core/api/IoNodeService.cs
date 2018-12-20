using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using NLog;
using NLog.Targets;
using zero.core.api.interfaces;
using zero.core.api.models;
using zero.core.core;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.protocol;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;

namespace zero.core.api
{
    /// <summary>
    /// Node services
    /// </summary>
    /// <seealso cref="Microsoft.AspNetCore.Mvc.Controller" />
    /// <seealso cref="zero.core.api.interfaces.IIoNodeService" />
    [EnableCors("ApiCorsPolicy")]
    [ApiController]
    //[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("/api/node")]
    public class IoNodeService<TBlob> : Controller, IIoNodeService 
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoNodeService{TBlob}"/> class.
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
        private static readonly ConcurrentDictionary<int, IoNode<IoTangleMessage<TBlob>>> _nodes = new ConcurrentDictionary<int, IoNode<IoTangleMessage<TBlob>>>();
        /// <summary>
        /// Posts the specified address.
        /// </summary>
        /// <param name="address">The listening address to start a node on</param>
        /// <returns>true on success, false otherwise</returns>
        [HttpPost]
        public IoApiReturn Post(IoNodeAddress address)
        {
            if (!address.IsValid)
                return IoApiReturn.Result(false, address.ValidationErrorString);

            if (!_nodes.TryAdd(address.Port, new IoNode<IoTangleMessage<TBlob>>(address, ioNetClient => new TanglePeer<TBlob>(ioNetClient))))
            {
                var errStr = $"Cannot create node `${address.UrlAndPort}', a node with that id already exists";
                _logger.Warn(errStr);
                return IoApiReturn.Result(true, errStr);
            }


            var dbgStr = $"Added listener at `{address.UrlAndPort}'";

            _nodes[address.Port].Start();

#pragma warning disable 4014
            _nodes[address.Port].SpawnConnectionAsync(IoNodeAddress.Create("tcp://unimatrix.uksouth.cloudapp.azure.com:15600"));
#pragma warning restore 4014

            _logger.Debug(dbgStr);
            return IoApiReturn.Result(true, dbgStr, address.Port);
        }

        [Route("/api/node/logs")]
        [HttpGet]
        public IoApiReturn Logs()
        {
            var data = _apiLogger.Logs.Select(l => new IoLogEntry(l)).ToList();
            var retval = IoApiReturn.Result(true, $"{data.Count} {nameof(Logs)} returned", data);
            _apiLogger.Logs.Clear();
            return retval;
        }

        [Route("/api/node/stream/{id}")]
        [HttpGet]
        public IoApiReturn TransactionStreamQuery([FromRoute]int id, [FromQuery] string tagQuery)
        {
            try
            {
                if (!_nodes.ContainsKey(id))
                    return IoApiReturn.Result(false, $"Could not find listener with id=`{id}'");
                var transactions = new List<IIoInteropTransactionModel<TBlob>>();

                int count = 0;
                long outstanding = 0;
                long freeBufferSpace = 0;

                Stopwatch stopwatch = new Stopwatch();
#pragma warning disable 4014 //TODO figure out what is going on with async
                _nodes.SelectMany(n => n.Value.Neighbors).Where(n => n.Key == id).Select(n => n.Value).ToList()
                    .ForEach(async n =>
#pragma warning restore 4014
                    {
                        var relaySource = n.PrimaryProducer.GetRelaySource<IoTangleTransaction<TBlob>>(nameof(IoNodeService<TBlob>));

                        if (relaySource != null)
                        {
                            stopwatch.Start();
                            count = 0;
                            while (Interlocked.Read(ref relaySource.JobMetaHeap.ReferenceCount) > 0)
                            {
                                await relaySource.ConsumeAsync(message =>
                                {
                                    if (message == null)
                                        return;


                                    var msg = ((IoTangleTransaction<TBlob>)message);

                                    if (count > 50)
                                        return;

                                    if (msg.Transactions == null)
                                        return;
                                    try
                                    {
                                        foreach (var t in msg.Transactions)
                                        {
                                            var tagStr = t.AsTrytes(t.Tag);

                                            if (tagQuery == null)
                                                transactions.Add(t);
                                            else if (!string.IsNullOrEmpty(tagStr) && tagStr.IndexOf(tagQuery, 0, StringComparison.CurrentCultureIgnoreCase) != -1)
                                                transactions.Add(t);
                                            else if (string.IsNullOrEmpty(tagStr) && string.IsNullOrEmpty(tagQuery))
                                            {
                                                transactions.Add(t);
                                            }
                                            if (++count > 50)
                                                break;
                                        }

                                        msg.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.Consumed;
                                    }
                                    finally
                                    {
                                        if (msg.ProcessState == IoProduceble<IoTangleTransaction<TBlob>>.State.Consuming)
                                            msg.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.ConsumeErr;
                                    }
                                }, sleepOnProducerLag: false);
                            }
                            stopwatch.Stop();
                            outstanding = relaySource.JobMetaHeap.ReferenceCount;
                            freeBufferSpace = relaySource.JobMetaHeap.FreeCapacity();
                        }
                        else
                            _logger.Warn($"Waiting for multicast producer `{n.PrimaryProducer.Description}' to initialize...");
                    });
                return IoApiReturn.Result(true, $"Queried listener at port `{id}', found `{transactions.Count}' transactions, scanned= `{count}', backlog= `{outstanding}', free= `{freeBufferSpace}', t= `{stopwatch.ElapsedMilliseconds} ms'", transactions, stopwatch.ElapsedMilliseconds);
            }
            catch (Exception e)
            {
                _logger.Error(e, $"{nameof(TransactionStreamQuery)} failed:");
                return IoApiReturn.Result(false, e.Message);
            }
            //TODO remove diagnostic output            
        }

        [Route("/api/node/stopListener/{id}")]
        [HttpGet]
        public IoApiReturn StopListener([FromRoute] int id)
        {
            if (!_nodes.ContainsKey(id))
                return IoApiReturn.Result(false, $"Neighbor with listener port `{id}' does not exist");

            _nodes[id].Stop();
            _nodes.TryRemove(id, out _);

            return IoApiReturn.Result(true, $"Successfully stopped neighbor `{id}'");
        }
    }
}
