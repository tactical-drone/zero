﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MathNet.Numerics.Distributions;
using NLog;
using zero.core.misc;
using zero.core.models;
using zero.interop.utils;
using zero.tangle.data.cassandra.tangle;
using zero.tangle.data.cassandra.tangle.luts;
using zero.tangle.entangled;
using zero.tangle.models;

namespace zero.tangle.utils
{
    /// <summary>
    /// Does coo milestone related stuff
    /// </summary>
    /// <typeparam name="TKey">The key type</typeparam>
    public class Milestone<TKey>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public Milestone(CancellationToken cancel,int degree = 2)
        {
            _parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = degree,
                CancellationToken = cancel
            };

            _parallelNone = new ParallelOptions
            {
                MaxDegreeOfParallelism = 1,
                CancellationToken = cancel
            };

            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        //Parallel options
        private readonly ParallelOptions _parallelOptions;

        private readonly ParallelOptions _parallelNone;

        /// <summary>
        /// All new transactions guess this milestone offset from current should confirm them //TODO what is this should?
        /// </summary>
        protected long InitialMilestoneDepthEstimate { get; set; } = 10;

        /// <summary>
        /// The expected time between issued milestone //TODO adjust from data
        /// </summary>
        protected long AveMilestoneSeconds { get; set; } = 120;

        /// <summary>
        /// Countermeasures for worst case scenarios
        /// </summary>
        private readonly Poisson _yield = new Poisson(1.0/ (Math.Min(Environment.ProcessorCount, 2) * 16));
        

        /// <summary>
        /// Walks a tree of <see cref="IoApprovedTransaction{TKey}"/> executing <paramref name="relaxTransaction"/> if needed
        /// </summary>
        /// <param name="tree">The tree</param>
        /// <param name="transactions">Transactions to be walked</param>
        /// <param name="currentMilestone">Current milestone</param>
        /// <param name="relaxTransaction">The relax step</param>
        /// <param name="depth">The current depth</param>
        private long Walker(ConcurrentDictionary<TKey, ConcurrentBag<IoApprovedTransaction<TKey>>> tree, ConcurrentBag<IoApprovedTransaction<TKey>> transactions, IoApprovedTransaction<TKey> currentMilestone,
            Action<ConcurrentBag<IoApprovedTransaction<TKey>>, IoApprovedTransaction<TKey>, long, long> relaxTransaction, long stackDepth = 0, long depth = 0)
        {
            Parallel.ForEach(transactions, depth == 0? _parallelOptions:_parallelNone, transaction =>
            {
                var locked = true;
                var currentDepth = depth;
                var entryStackDepth = stackDepth;
                try
                {                    
                    if(_yield.Sample() > 0)
                        Thread.Sleep(_yield.Sample());

                    Monitor.Enter(transaction);
                    if (transaction.Walked == false && (transaction.Walked = true) ||                        
                        transaction.Depth > depth && transaction.TotalDepth >= entryStackDepth)
                    {
                        Monitor.Exit(transaction);
                        locked = false;

                        var nextMilestone = currentMilestone;
                        //set your milestone
                        if (transaction.IsMilestone)
                        {
                            nextMilestone = transaction;
                            currentDepth = 0;
                        }
                        
                        //walk the tree
                        if (tree.TryGetValue(transaction.Hash, out var children))
                        {
                            if (children.Any())
                            {                                         
                                relaxTransaction(children, nextMilestone, currentDepth + 1, entryStackDepth + 1);                                                                
                                Volatile.Write(ref stackDepth, Math.Max((int)Walker(tree, children, nextMilestone, relaxTransaction, Interlocked.Read(ref stackDepth) + 1, currentDepth + 1), Interlocked.Read(ref stackDepth)));                                
                            }
                        }                                                
                    }
                }
                catch(Exception e)
                {
                    _logger.Error(e,$"Walker> m = `{currentMilestone.MilestoneIndexEstimate}', h = [{transaction.Hash}] , s = `{stackDepth}', d = `{depth}' :");
                }
                finally
                {
                    if (locked)
                        Monitor.Exit(transaction);
                }
            });

            return stackDepth;
        }

        /// <summary>
        /// Update milestone mechanics
        /// </summary>
        /// <param name="node">The node that manages milestones</param>
        /// <param name="dataSource">The source where milestone data can be found</param>
        /// <param name="transaction">The latest transaction</param>
        /// <returns>Task</returns>
        public async Task UpdateIndexAsync(TangleNode<IoTangleMessage<TKey>, TKey> node, IoTangleCassandraDb<TKey> dataSource, IIoTransactionModel<TKey> transaction)
        {            
            transaction.MilestoneIndexEstimate = 0;
            
            //Update latest seen milestone transaction
            if (node.LatestMilestoneTransaction == null && transaction.AsTrytes(transaction.AddressBuffer) == node.parm_coo_address
                || node.LatestMilestoneTransaction != null && transaction.AddressBuffer.AsArray().SequenceEqual(node.LatestMilestoneTransaction.AddressBuffer.AsArray())
               )
            {
                transaction.SecondsToMilestone = 0;
                transaction.IsMilestoneTransaction = true;
                transaction.MilestoneEstimateTransaction = transaction;
                transaction.MilestoneIndexEstimate = transaction.GetMilestoneIndex();

                if (transaction.Timestamp > (node.LatestMilestoneTransaction?.Timestamp ?? 0))
                {
                    node.LatestMilestoneTransaction = transaction;

                    var timeDiff = DateTime.Now - transaction.Timestamp.DateTime();
                    _logger.Info(Entangled<TKey>.Optimized
                        ? $"[{transaction.Timestamp.DateTime()}]: New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.AsTrytes(transaction.HashBuffer)}]"
                        : $"[{transaction.Timestamp.DateTime()}]: New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.Hash}]");
                }
            }
            //Load from the DB if we don't have one ready
            else if (node.LatestMilestoneTransaction == null)
            {
                node.LatestMilestoneTransaction = await dataSource.GetBestMilestoneEstimateBundle(((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds());

                if (node.LatestMilestoneTransaction != null)
                {
                    var timeDiff = DateTime.Now - node.LatestMilestoneTransaction.Timestamp.DateTime();
                    _logger.Debug(Entangled<TKey>.Optimized
                        ? $"Loaded latest milestoneIndex = `{node.LatestMilestoneTransaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{node.LatestMilestoneTransaction.AsTrytes(node.LatestMilestoneTransaction.HashBuffer)}]"
                        : $"Loaded latest milestoneIndex = `{node.LatestMilestoneTransaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{node.LatestMilestoneTransaction.Hash}]");
                }
                else
                {
                    //_logger.Trace($"Unable to load nearest milestone for t = `{((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds()}'");
                }
            }

            //If this is a milestone transaction there is nothing more to be done
            if (transaction.IsMilestoneTransaction)
                return;

            //set transaction milestone estimate if the transaction is newer than newest milestone seen
            if (node.LatestMilestoneTransaction != null && node.LatestMilestoneTransaction.Timestamp <= transaction.Timestamp)
            {
                transaction.MilestoneIndexEstimate = node.LatestMilestoneTransaction.GetMilestoneIndex() + InitialMilestoneDepthEstimate;
                transaction.SecondsToMilestone = InitialMilestoneDepthEstimate * AveMilestoneSeconds;
            }
            else //look for a candidate milestone in storage for older transactions //TODO make this better for a dup?
            {
                var stopwatch = Stopwatch.StartNew();
                var relaxMilestone = await dataSource.GetBestMilestoneEstimateBundle(transaction.Timestamp + AveMilestoneSeconds * InitialMilestoneDepthEstimate * 1000);
                stopwatch.Stop();

                try
                {
                    if (relaxMilestone != null)
                    {
                        //_logger.Trace($"Attached milestone: `{relaxMilestone.MilestoneIndexEstimate = relaxMilestone.GetMilestoneIndex()}', dt = `{relaxMilestone.Timestamp.DateTime().DateTime - transaction.Timestamp.DateTime().DateTime}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                    }                        
                    else
                    {
                        try
                        {
                            //_logger.Trace($"Milestone not found: `{transaction.Timestamp}' = `{transaction.Timestamp.DateTime().DateTime}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                        }
                        catch
                        {
                            //_logger.Trace($"Milestone not found: `{transaction.Timestamp}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                        }
                        return;
                    }

                    var secondsToMilestone = (long)(relaxMilestone.GetAttachmentTime().DateTime() - transaction.GetAttachmentTime().DateTime()).TotalSeconds;
                    if (secondsToMilestone > InitialMilestoneDepthEstimate * AveMilestoneSeconds)
                    {
                        transaction.MilestoneIndexEstimate = relaxMilestone.GetMilestoneIndex();
                        transaction.SecondsToMilestone = (long)(relaxMilestone.GetAttachmentTime().DateTime() - transaction.GetAttachmentTime().DateTime()).TotalSeconds;
                    }
                }
                catch (Exception e)
                {
                    _logger.Trace($"Cannot find milestone for invalid date: `{transaction.Timestamp.DateTime()}'");
                }
            }
        }

        /// <summary>
        /// Relax transactions towards <paramref name="rootMilestone"/>
        /// </summary>
        /// <param name="ioApprovedTransactions">The transactions to be processed</param>
        /// <param name="rootMilestone">The root milestone</param>
        /// <returns>A list of all transactions that were relaxed</returns>
        public ConcurrentBag<IoApprovedTransaction<TKey>> Relax(IoApprovedTransaction<TKey>[] ioApprovedTransactions, IIoTransactionModel<TKey> rootMilestone)
        {
            var relaxedTransactions = new ConcurrentBag<IoApprovedTransaction<TKey>>();            

            //Prepare the tree
            var tree = new ConcurrentDictionary<TKey, ConcurrentBag<IoApprovedTransaction<TKey>>>();
            var stopwatch = Stopwatch.StartNew();

            try
            {
                Parallel.ForEach(ioApprovedTransactions, _parallelOptions, t =>
                {
                    if (!tree.TryAdd(t.Verifier, new ConcurrentBag<IoApprovedTransaction<TKey>>(new[] { t }.ToList())))
                    {
                        tree[t.Verifier].Add(t);
                    }
                });
            }
            catch (Exception e)
            {
                _logger.Error(e, "Prepare tree: ");
                return relaxedTransactions;
            }

            stopwatch.Stop();
            _logger.Debug($"Preparing milestones: t = `{stopwatch.ElapsedMilliseconds}ms', v = `{tree.Count}', {tree.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D}/tps");

            stopwatch.Restart();

            long loads = 0;
            long scans = 0;
            long dagFail = 0;
            long totalStack = 0;
            //Relax transaction milestones
            if (tree.ContainsKey(rootMilestone.Hash))
            {
                totalStack = Walker(tree, tree[rootMilestone.Hash], tree[rootMilestone.Hash].First(),
  (transactions, currentMilestone, depth, totalDepth) =>
                {
                    try
                    {
                        foreach (var transaction in transactions)
                        {
                            Interlocked.Increment(ref scans);
                            {
                                lock (transaction)
                                {
                                    if (transaction.Depth > depth )//&& transaction.TotalDepth >= totalDepth) //TODO Do we want shortest path?
                                    {
                                        if (transaction.TotalDepth < totalDepth)
                                        {
                                            dagFail++;
                                            continue;                                            
                                        }

                                        if (!transaction.IsMilestone)
                                        {
                                            transaction.MilestoneIndexEstimate =
                                                currentMilestone.MilestoneIndexEstimate;
                                        }

                                        transaction.SecondsToMilestone =
                                            (long) (currentMilestone.Timestamp.DateTime() -
                                                    transaction.Timestamp.DateTime()).TotalSeconds;
                                        transaction.Depth = depth;
                                        transaction.TotalDepth = totalDepth;

                                        Interlocked.Increment(ref loads);
                                        if (!transaction.Loaded)
                                        {
                                            relaxedTransactions.Add(transaction);
                                            transaction.Loaded = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, "Walker relax: ");
                    }
                });
            }

            stopwatch.Stop();

            _logger.Debug($"Relax transaction milestones: s = `{totalStack}', t = `{stopwatch.ElapsedMilliseconds}ms', c = `{dagFail}/{loads}/{scans}', {scans * 1000 / (stopwatch.ElapsedMilliseconds + 1):D}/sps");

            return relaxedTransactions;
        }
    }
}
