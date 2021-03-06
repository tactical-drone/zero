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
using zero.core.core;
using zero.core.feat.api;
using zero.core.feat.api.models;
using zero.core.network.ip;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.interop.entangled.common.model;
using zero.tangle.api.interfaces;
using zero.tangle.models;

namespace zero.tangle.api.controllers.generic
{
    /// <summary>
    /// Node services
    /// </summary>
    /// <seealso cref="Controller" />
    /// <seealso cref="IIoNodeController" />
    [EnableCors("ApiCorsPolicy")]

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
        private static readonly ConcurrentDictionary<int, IoNode<IoTangleMessage<TKey>>> Nodes = new();
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

            if (!Nodes.TryAdd(address.Port, new IoNode<IoTangleMessage<TKey>>(address, (node, ioNetClient, extraData) => new TanglePeer<TKey>((TangleNode<IoTangleMessage<TKey>, TKey>) node, ioNetClient), TanglePeer<TKey>.TcpReadAhead, 2, 16)))
            {
                var errStr = $"Cannot create node `${address.Url}', a node with that id already exists";
                _logger.Warn(errStr);
                return IoApiReturn.Result(true, errStr);
            }


            var dbgStr = $"Added listener at `{address.Url}'";

#pragma warning disable 4014
            var task = Nodes[address.Port].StartAsync();
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
                var task = Nodes.SelectMany(n => n.Value.Neighbors).Select(n => n.Value).ToList()
                    .ForEachAsync(static async (n,state) =>
                    {
                        var (@this, stopwatch,count,tagQuery,transactions,outstanding,freeBufferSpace) =state;
                        var relaySource = await n.Source.CreateConduitOnceAsync<IoTangleTransaction<TKey>>(nameof(IoNodeServices<TKey>)).ConfigureAwait(false);

                        if (relaySource != null)
                        {
                            stopwatch.Start();
                            Interlocked.Exchange(ref count, 0);
                            while (relaySource.JobHeap.ReferenceCount > 0)
                            {
                                await relaySource.ConsumeAsync<object>((message,_) =>
                                {
                                    if (message == null)
                                        return default;


                                    var msg = ((IoTangleTransaction<TKey>)message);

                                    if (Volatile.Read(ref count) > 50)
                                        return default;

                                    if (msg.Transactions == null)
                                        return default;
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
                                            if (Interlocked.Increment(ref count) > 50)
                                                break;
                                        }

                                        msg.State = IoJobMeta.JobState.Consumed;
                                    }
                                    finally
                                    {
                                        if (msg.State == IoJobMeta.JobState.Consuming)
                                            msg.State = IoJobMeta.JobState.ConsumeErr;
                                    }

                                    return default;                                    
                                }).FastPath().ConfigureAwait(false);
                            }
                            stopwatch.Stop();
                            Interlocked.Exchange(ref outstanding, relaySource.JobHeap.ReferenceCount);
                            Interlocked.Exchange(ref freeBufferSpace, relaySource.JobHeap.AvailableCapacity);
                        }
                        else
                            @this._logger.Warn($"Waiting for multicast source `{n.Source.Description}' to initialize...");
                    }, ValueTuple.Create(this,stopwatch,count,tagQuery,transactions,outstanding,freeBufferSpace));
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

            Nodes[id].Zero(null, "MAIN TEARDOWN").AsTask().GetAwaiter().GetResult();
            Nodes.TryRemove(id, out _);

            return IoApiReturn.Result(true, $"Successfully stopped neighbor `{id}'");
        }
    }
}
