using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MathNet.Numerics.Distributions;
using NLog;
using zero.core.misc;
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
        public long AveMilestoneSeconds { get; protected set; } = 180;

        /// <summary>
        /// Countermeasures for worst case scenarios
        /// </summary>
        private readonly Poisson _yield = new Poisson(1.0/ (Math.Min(Environment.ProcessorCount, 2) * 16));


        /// <summary>
        /// Walks a tree of <see cref="IoApprovedTransaction{TKey}"/> executing <paramref name="relaxTransaction"/> if needed
        /// </summary>
        /// <param name="tree">The tree</param>
        /// <param name="currentMilestone">Current milestone</param>
        /// <param name="entryPoint"></param>
        /// <param name="relaxTransaction">The relax step</param>
        /// <param name="stack">Keeps track of current stack depth</param>
        /// <param name="depth">The current depth being relaxed</param>
        private int Walker(ConcurrentDictionary<TKey, ConcurrentBag<IoApprovedTransaction<TKey>>> tree, TKey entryPoint, 
            Action<ConcurrentBag<IoApprovedTransaction<TKey>>, IoApprovedTransaction<TKey>, int, int> relaxTransaction, IoApprovedTransaction<TKey> currentMilestone = null, int stack = 0, int depth = 0)
        {                        
            if (tree.TryGetValue(entryPoint, out var transactions))
            {
                relaxTransaction(transactions, currentMilestone, depth + 1, stack + 1);

                var entryDepth = depth;
                var entryStack = stack;                

                Parallel.ForEach(transactions, depth == 0? _parallelOptions:_parallelNone, transaction =>
                {
                    var locked = true;
                    
                    try
                    {
                        if (_yield.Sample() > 0)
                            Thread.Sleep(_yield.Sample());
                                                    
                        Monitor.Enter(transaction);
                        if (transaction.Walked == false && (transaction.Walked = true) ||                                                        
                            transaction.Height > depth &&
                            (transaction.IsMilestone || transaction.Milestone < 1 || transaction.Milestone > currentMilestone.Milestone))
                        {
                            Monitor.Exit(transaction);
                            locked = false;

                            //relax milestone
                            var nextMilestone = currentMilestone;                            
                            if (transaction.IsMilestone)
                            {                                
                                nextMilestone = transaction;                                
                                entryDepth = 0;
                            }

                            //relax transactions
                            stack = Math.Max((int)Walker(tree, transaction.Hash, relaxTransaction, nextMilestone, entryStack + 1, entryDepth + 1), stack);
                        }
                    }
                    catch(Exception e)
                    {
                        _logger.Error(e,$"Walker> m = `{currentMilestone.Milestone}', h = [{transaction.Hash}] , s = `{stack}', d = `{depth}' :");
                    }
                    finally
                    {
                        if (locked)
                            Monitor.Exit(transaction);
                    }
                });
            }            

            return stack;
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
                transaction.ConfirmationTime = 0;
                transaction.IsMilestoneTransaction = true;
                transaction.MilestoneEstimateTransaction = transaction;
                transaction.MilestoneIndexEstimate = transaction.GetMilestoneIndex();

                if (transaction.GetAttachmentTime() > (node.LatestMilestoneTransaction?.GetAttachmentTime() ?? 0))
                {
                    node.LatestMilestoneTransaction = transaction;

                    var timeDiff = DateTime.Now - transaction.GetAttachmentTime().DateTime();
                    _logger.Info($"{transaction.AsTrytes(transaction.HashBuffer)} : New milestoneIndex = `{transaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{transaction.GetAttachmentTime().DateTime().ToLocalTime()}]");
                }
            }
            //Load from the DB if we don't have one ready
            else if (node.LatestMilestoneTransaction == null)
            {
                node.LatestMilestoneTransaction = await dataSource.GetBestMilestoneEstimateBundle(((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds());

                if (node.LatestMilestoneTransaction != null)
                {
                    var timeDiff = DateTime.Now - node.LatestMilestoneTransaction.Timestamp.DateTime();
                    _logger.Debug($"Loaded latest milestoneIndex = `{node.LatestMilestoneTransaction.GetMilestoneIndex()}', dt = `{timeDiff}': [{node.LatestMilestoneTransaction.AsKeyString(node.LatestMilestoneTransaction.HashBuffer)}]");
                }
                else
                {
                    _logger.Trace($"Unable to load nearest milestone for t = `{((DateTimeOffset)DateTime.Now).ToUnixTimeMilliseconds()}'");
                }
            }

            //If this is a milestone transaction there is nothing more to be done
            if (transaction.IsMilestoneTransaction)
                return;
            else //we don't need the full obsolete tag anymore
            {
                var preStrippedSize = transaction.ObsoleteTagBuffer.Length;
                transaction.ObsoleteTag = transaction.Trimmed(transaction.ObsoleteTag);
                transaction.Size -= (short)(preStrippedSize - transaction.ObsoleteTagBuffer.Length);
            }

            //set transaction milestone estimate if the transaction is newer than newest milestone seen
            if (node.LatestMilestoneTransaction != null && node.LatestMilestoneTransaction.Timestamp <= transaction.Timestamp)
            {
                transaction.MilestoneIndexEstimate = node.LatestMilestoneTransaction.GetMilestoneIndex() + InitialMilestoneDepthEstimate;
                //transaction.SecondsToMilestone = InitialMilestoneDepthEstimate * AveMilestoneSeconds;
                transaction.ConfirmationTime = 0;
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
                        _logger.Trace($"Estimated milestone for [{transaction.AsKeyString(transaction.HashBuffer)}]: `{relaxMilestone.MilestoneIndexEstimate = relaxMilestone.GetMilestoneIndex()}', dt = `{relaxMilestone.Timestamp.DateTime().DateTime - transaction.Timestamp.DateTime().DateTime}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                    }                        
                    else
                    {
                        try
                        {
                            _logger.Trace($"Milestone not found [{transaction.AsKeyString(transaction.HashBuffer)}]: `{transaction.Timestamp}' = `{transaction.Timestamp.DateTime().DateTime}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                        }
                        catch
                        {
                            _logger.Trace($"Milestone not found [{transaction.AsKeyString(transaction.HashBuffer)}]: `{transaction.Timestamp}', t = `{stopwatch.ElapsedMilliseconds}ms'");
                        }
                        return;
                    }

                    var secondsToMilestone = (long)(relaxMilestone.GetAttachmentTime().DateTime() - transaction.GetAttachmentTime().DateTime()).TotalSeconds;
                    if (secondsToMilestone > InitialMilestoneDepthEstimate * AveMilestoneSeconds)
                    {
                        transaction.MilestoneIndexEstimate = relaxMilestone.GetMilestoneIndex();
                        //transaction.SecondsToMilestone = (long)(relaxMilestone.GetAttachmentTime().DateTime() - transaction.GetAttachmentTime().DateTime()).TotalSeconds;
                        transaction.ConfirmationTime = 0;
                    }
                }
                catch (Exception e)
                {
                    _logger.Trace($"Cannot find milestone for invalid date: `{transaction.Timestamp}'");
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
            ConcurrentDictionary<TKey, ConcurrentBag<IoApprovedTransaction<TKey>>> tree = Entangled<TKey>.Optimized ? new ConcurrentDictionary<TKey, ConcurrentBag<IoApprovedTransaction<TKey>>>((IEqualityComparer<TKey>)new IoByteArrayComparer()) : new ConcurrentDictionary<TKey, ConcurrentBag<IoApprovedTransaction<TKey>>>();

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
                _logger.Error(e, "Prepare tree hash: ");
                return relaxedTransactions;
            }

            stopwatch.Stop();
            _logger.Debug($"Tree hash: t = `{stopwatch.ElapsedMilliseconds}ms', v = `{tree.Count}', {tree.Count * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} n/s");

            stopwatch.Restart();

            int hits = 0;            
            int scans = 0;            
            int totalStack = 0;
            int aveConfTime = 0;
            int aveConfTimeCount = 0;
            int milestonesCrossed = 0;
            //Relax transaction milestones
            if (tree.TryGetValue(rootMilestone.Hash, out var children))
            {
                var entryPoint = children.First();
                totalStack = Walker(tree, entryPoint.Verifier,
  (transactions, currentMilestone, depth, totalDepth) =>
                {
                    try
                    {
                        foreach (var transaction in transactions)
                        {
                            Interlocked.Increment(ref scans);                            
                            lock (transaction)
                            {
                                if (transaction.Height > depth && (transaction.IsMilestone || transaction.Milestone < 1  || transaction.Milestone > currentMilestone.Milestone))
                                {                                    
                                    if (transaction.IsMilestone)
                                    {
                                        //coo consensus
                                        if (transaction.Milestone == currentMilestone.Milestone - 1)
                                        {
                                            transaction.Depth = depth;
                                        }

                                        if(!transaction.Walked)
                                            milestonesCrossed++;
                                    }
                                    else //milestone consensus
                                    {
                                        transaction.Milestone = currentMilestone.Milestone;
                                        transaction.Depth = depth;
                                    }
                                         
                                    //scanned
                                    transaction.Height = depth;
                                    
                                    //confirmation time
                                    if (transaction.ConfirmationTime == 0)
                                    {
                                        aveConfTime += transaction.ConfirmationTime = (int) (DateTime.UtcNow - transaction.Timestamp.DateTime()).TotalSeconds;
                                        aveConfTimeCount++;
                                    }
                                       
                                    //load                                    
                                    if (!transaction.Loaded && transaction.HasChanges)
                                    {
                                        relaxedTransactions.Add(transaction);
                                        transaction.Loaded = true;
                                    }

                                    Interlocked.Increment(ref hits);
                                }                                    
                            }                                                            
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Error(e, "Walker relax: ");
                    }
                }, tree[rootMilestone.Hash].First());
            }
            else
            {
                _logger.Warn($"Could not find milestone entrypoint for m = `{rootMilestone.MilestoneIndexEstimate}', [{rootMilestone.AsKeyString(rootMilestone.HashBuffer)}] - `{rootMilestone.GetAttachmentTime().DateTime()}'");                
            }

            stopwatch.Stop();

            _logger.Debug($"Relax transactions: ct = `{(aveConfTime/Math.Max(aveConfTimeCount,1))/60.0:F} min', d = `{totalStack}', mx = `{milestonesCrossed}', t = `{stopwatch.ElapsedMilliseconds}ms', c = `{hits}/{scans}', {scans * 1000 / (stopwatch.ElapsedMilliseconds + 1):D} s/t");

            return relaxedTransactions;
        }
    }
}
