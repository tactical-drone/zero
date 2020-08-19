using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using NLog;
using NLog.Targets;
using zero.core.api;
using zero.core.api.models;
using zero.core.core;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.interop.entangled.common.model;
using zero.tangle.api.interfaces;
using zero.tangle.models;

namespace zero.tangle.api.controllers.generic
{
    /// <summary>
    /// Node services
    /// </summary>
    /// <seealso cref="Microsoft.AspNetCore.Mvc.Controller" />
    /// <seealso cref="IIoNodeController" />
    [EnableCors("ApiCorsPolicy")]
    [ApiController]
    //[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]    
    public class IoNodeServices<TKey> : Controller, IIoNodeController
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IoNodeServices{TKey}"/> class.
        /// </summary>
        public IoNodeServices()
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
        private static readonly ConcurrentDictionary<int, IoNode<IoTangleMessage<TKey>>> Nodes = new ConcurrentDictionary<int, IoNode<IoTangleMessage<TKey>>>();
        /// <summary>
        /// Posts the specified address.
        /// </summary>
        /// <param name="address">The listening address to start a node on</param>
        /// <returns>true on success, false otherwise</returns>
        [HttpPost]        
        public IoApiReturn Post(IoNodeAddress address)
        {
            if (!address.Validated)
                return IoApiReturn.Result(false, address.ValidationErrorString);

            if (!Nodes.TryAdd(address.Port, new IoNode<IoTangleMessage<TKey>>(address, (node, ioNetClient, extraData) => new TanglePeer<TKey>((TangleNode<IoTangleMessage<TKey>, TKey>) node, ioNetClient), TanglePeer<TKey>.TcpReadAhead)))
            {
                var errStr = $"Cannot create node `${address.Url}', a node with that id already exists";
                _logger.Warn(errStr);
                return IoApiReturn.Result(true, errStr);
            }


            var dbgStr = $"Added listener at `{address.Url}'";

#pragma warning disable 4014
            Nodes[address.Port].StartAsync();
#pragma warning restore 4014

            _logger.Debug(dbgStr);
            return IoApiReturn.Result(true, dbgStr, address.Port);
        }

        [Route("logs")]
        [HttpGet]
        public IoApiReturn Logs()
        {
            var data = _apiLogger.Logs.Select(l => new IoLogEntry(l)).ToList();
            var retval = IoApiReturn.Result(true, $"{data.Count} {nameof(Logs)} returned", data);
            _apiLogger.Logs.Clear();
            return retval;
        }

        [Route("stream/{id}")]
        [HttpGet]
        public IoApiReturn TransactionStreamQuery([FromRoute]int id, [FromQuery] string tagQuery)
        {
            try
            {
                if (!Nodes.ContainsKey(id))
                    return IoApiReturn.Result(false, $"Could not find listener with id=`{id}'");
                var transactions = new List<IIoTransactionModel<TKey>>();

                int count = 0;
                long outstanding = 0;
                long freeBufferSpace = 0;

                Stopwatch stopwatch = new Stopwatch();
#pragma warning disable 4014 //TODO figure out what is going on with async
                Nodes.SelectMany(n => n.Value.Neighbors).Select(n => n.Value).ToList()
                    .ForEach(async n =>
#pragma warning restore 4014
                    {
                        var relaySource = n.Source.AttachProducer<IoTangleTransaction<TKey>>(nameof(IoNodeServices<TKey>));

                        if (relaySource != null)
                        {
                            stopwatch.Start();
                            count = 0;
                            while (Interlocked.Read(ref relaySource.JobHeap.ReferenceCount) > 0)
                            {
                                await relaySource.ConsumeAsync(message =>
                                {
                                    if (message == null)
                                        return Task.CompletedTask;


                                    var msg = ((IoTangleTransaction<TKey>)message);

                                    if (count > 50)
                                        return Task.CompletedTask;

                                    if (msg.Transactions == null)
                                        return Task.CompletedTask;
                                    try
                                    {
                                        foreach (var t in msg.Transactions)
                                        {
                                            var tagStr = t.AsTrytes(t.TagBuffer, IoTransaction.NUM_TRITS_OBSOLETE_TAG, IoTransaction.NUM_TRYTES_OBSOLETE_TAG);

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

                                        msg.ProcessState = IoJob<IoTangleTransaction<TKey>>.State.Consumed;
                                    }
                                    finally
                                    {
                                        if (msg.ProcessState == IoJob<IoTangleTransaction<TKey>>.State.Consuming)
                                            msg.ProcessState = IoJob<IoTangleTransaction<TKey>>.State.ConsumeErr;
                                    }

                                    return Task.CompletedTask;                                    
                                }, sleepOnProducerLag: false);
                            }
                            stopwatch.Stop();
                            outstanding = relaySource.JobHeap.ReferenceCount;
                            freeBufferSpace = relaySource.JobHeap.FreeCapacity();
                        }
                        else
                            _logger.Warn($"Waiting for multicast source `{n.Source.Description}' to initialize...");
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

        [Route("stopListener/{id}")]
        [HttpGet]
        public IoApiReturn StopListener([FromRoute] int id)
        {
            if (!Nodes.ContainsKey(id))
                return IoApiReturn.Result(false, $"Neighbor with listener port `{id}' does not exist");

            Nodes[id].Close();
            Nodes.TryRemove(id, out _);

            return IoApiReturn.Result(true, $"Successfully stopped neighbor `{id}'");
        }
    }
}
