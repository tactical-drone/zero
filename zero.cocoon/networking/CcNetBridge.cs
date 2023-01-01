using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NLog;
using zero.cocoon.autopeer;
using zero.cocoon.identity;
using zero.cocoon.models;
using zero.cocoon.models.batches;
using zero.core.feat.models.bundle;
using zero.core.feat.models.protobuffer;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;
using zero.core.runtime.scheduler;
using Zero.Models.Protobuf;

namespace zero.cocoon.networking
{
    public class CcNetBridge : IoNanoprobe
    {
        public CcNetBridge(IoConduit<CcProtocBatchJob<chroniton, CcFrameBatch>> ioConduit)
        {
            _logger = LogManager.GetCurrentClassLogger();
            Channel = ioConduit;
        }

        private readonly Logger _logger;

        /// <summary>
        /// Message batch broadcast channel
        /// </summary>
        protected IoConduit<CcProtocBatchJob<chroniton, CcFrameBatch>> Channel;

        private static async ValueTask ProcessMessagesAsync(IoSink<CcProtocBatchJob<chroniton, CcFrameBatch>> batchJob, CcNetBridge @this)
        {
            //IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
            //{
            //var (@this, batchJob) = (ValueTuple< CcAdjunct , IoSink <CcProtocBatchJob<chroniton, CcDiscoveryBatch>>>)state;
            try
            {
                await @this.ZeroUnBatchAsync(batchJob, @this.Channel, static async (batchItem, @this, currentRoute) =>
                {
                    chroniton zero = default;
                    try
                    {
                        zero = batchItem.Payload;

                        if (zero == null)
                            return;


                        if (zero.Data.Length == 0)
                        {
                            @this._logger.Warn($"Got zero message from {CcDesignation.MakeKey(zero.PublicKey.Memory.AsArray())}");
                            return;
                        }

                        if (!@this.Zeroed() && !currentRoute.Zeroed())
                        {
                            try
                            {
                                var srcEndPoint = batchItem.EndPoint.GetEndpoint();
                                //switch ((CcDiscoveries.MessageTypes)MemoryMarshal.Read<int>(packet.Signature.Span))
                                switch ((CcDiscoveries.MessageTypes)zero.Type)
                                {
                                    default:
                                        break;
                                }
                            }
                            catch when (@this.Zeroed() || currentRoute.Zeroed()) { }
                            catch (Exception e) when (!@this.Zeroed() && !currentRoute.Zeroed())
                            {
                                @this._logger?.Error(e, $"{(CcDiscoveries.MessageTypes)zero.Type} [FAILED]: l = {zero!.Data.Length}, {@this.Description}");
                            }
                        }
                    }
                    catch when (@this.Zeroed()) { }
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        @this._logger?.Error(e, $"{zero} [FAILED]: l = {zero?.Data.Length}, {@this.Description}");
                    }
                }, @this).FastPath();
            }
            finally
            {
                if (batchJob != null && batchJob.State != IoJobMeta.JobState.Consumed)
                    await batchJob.SetStateAsync(IoJobMeta.JobState.ConsumeErr).FastPath();
            }
            //}, (@this, batchJob));
        }

        /// <summary>
        /// Processes protocol messages
        /// </summary>
        /// <returns></returns>
        private async ValueTask BridgeAsync()
        {
            try
            {
                //fail fast on these
                if (Zeroed() || Channel.Zeroed())
                    return;

                do
                {
                    //The consumer
                    var width = Channel.Source.ZeroConcurrencyLevel;
                    //var width = 1;
                    for (var i = 0; i < width; i++)
                        await ZeroAsync(static async state =>
                        {
                            var (@this, i) = (ValueTuple<CcNetBridge, int>)state;
                            try
                            {
                                while (!@this.Zeroed())
                                    await @this.Channel.ConsumeAsync(ProcessMessagesAsync, @this).FastPath();
                            }
                            catch when (@this.Zeroed() || @this.Channel?.UpstreamSource == null) { }
                            catch (Exception e) when (!@this.Zeroed() && @this.Channel?.UpstreamSource != null)
                            {
                                @this._logger?.Error(e, $"{@this.Description}");
                            }
                        }, (this, i)).FastPath();

                    //producer;
                    width = Channel.Source.PrefetchSize;
                    for (var i = 0; i < width; i++)
                        await ZeroAsync(static async @this =>
                        {
                            try
                            {
                                while (!@this.Zeroed())
                                {
                                    try
                                    {
                                        if (!await @this.Channel.ProduceAsync().FastPath())
                                            await Task.Delay(1000, @this.AsyncTasks.Token);
                                    }
                                    catch when (@this.Zeroed()) { }
                                    catch (Exception e) when (!@this.Zeroed())
                                    {
                                        @this._logger.Error(e, $"Production failed for {@this.Description}");
                                        break;
                                    }
                                }
                            }
                            catch when (@this.Zeroed() || @this.Channel?.UpstreamSource == null) { }
                            catch (Exception e) when (!@this.Zeroed())
                            {
                                @this._logger?.Error(e, $"{@this.Description}");
                            }

                        }, this).FastPath();

                    await AsyncTasks.BlockOnNotCanceledAsync().FastPath();
                }
                while (!Zeroed());
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger?.Debug(e, $"Error processing {Description}");
            }

            if (!Zeroed())
            {
                _logger?.Fatal($"{Description} Stopped prosessing discoveries.");
            }
        }

        /// <summary>
        /// Unpacks a batch of messages
        ///
        /// Batching strategies needs to be carefully matched with the nature of incoming traffic, or bad things will happen.
        /// </summary>
        /// <param name="batchJob">The consumer that need processing</param>
        /// <param name="channel">The arbiter</param>
        /// <param name="processCallback">The process callback</param>
        /// <param name="nanite"></param>
        /// <returns>Task</returns>
        private async ValueTask ZeroUnBatchAsync<T>(IoSink<CcProtocBatchJob<chroniton, CcFrameBatch>> batchJob,
            IoConduit<CcProtocBatchJob<chroniton, CcFrameBatch>> channel,
            Func<IIoBundleMessage, T, CcDrone, ValueTask> processCallback, T nanite)
        {
            if (batchJob == null)
                return;

            CcFrameBatch msgBatch = default;
            try
            {
                msgBatch = ((CcProtocBatchJob<chroniton, CcFrameBatch>)batchJob).Get();

                if (msgBatch == null)
                    return;

                //Grouped by ingress endpoint batches (this needs to make sense or disable)
                //if (msgBatch.GroupByEpEnabled)
                //{
                //    foreach (var epGroups in msgBatch.GroupBy)
                //    {
                //        if (Zeroed())
                //            break;
                //        if (msgBatch.GroupBy.TryGetValue(epGroups.Key, out var msgGroup))
                //        {
                //            if (!msgGroup.Item2.Any())
                //                continue;

                //            var srcEndPoint = epGroups.Value.Item1.GetEndpoint();
                //            var proxy = await RouteAsync(srcEndPoint, epGroups.Value.Item2[0].Zero.PublicKey).FastPath();
                //            if (proxy != null)
                //            {
                //                foreach (var message in msgGroup.Item2)
                //                {
                //                    //if (message.SourceState > 0)
                //                    //{
                //                    //    var routed = Router._routingTable.TryGetValue(message.EndPoint.GetEndpoint().ToString(), out var zombie);
                //                    //    if (routed)
                //                    //    {
                //                    //        await zombie.DisposeAsync(this, "Connection RESET!!!");
                //                    //        break;
                //                    //    }
                //                    //    continue;
                //                    //}


                //                    if (Zeroed())
                //                        break;
                //                    try
                //                    {
                //                        //if (Equals(message.Zero.Header.Ip.Dst.GetEndpoint(),
                //                        //        Router.MessageService.IoNetSocket.NativeSocket.RemoteEndPoint))
                //                        {
                //                            //IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
                //                            //{
                //                            //    var (processCallback, message, nanite, proxy, srcEndPoint) = (ValueTuple< Func < CcBatchMessage, T, CcAdjunct, IPEndPoint, ValueTask > , CcBatchMessage, T, CcAdjunct, IPEndPoint>)state;
                //                            await processCallback(message, nanite, proxy).FastPath();
                //                            //}, (processCallback, message, nanite, proxy, srcEndPoint));
                //                        }
                //                    }
                //                    catch (Exception) when (!Zeroed()) { }
                //                    catch (Exception e) when (Zeroed())
                //                    {
                //                        _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                //                    }
                //                    finally
                //                    {
                //                        message.Zero = null;
                //                    }
                //                }
                //            }
                //        }
                //        msgBatch.GroupBy.Clear();
                //    }
                //}
                //else //ZeroDefault and safe mode
                {
                    if (msgBatch.Count > msgBatch.Capacity * 2 / 3)
                        _logger.Warn($"{nameof(ZeroUnBatchAsync)}: -> large batches detected; size = {msgBatch.Count}/{msgBatch.Capacity}");

                    for (var i = 0; i < msgBatch.Count; i++)
                    {
                        if (Zeroed())
                            break;

                        try
                        {
                            await processCallback(msgBatch[i], nanite, msgBatch.Drone).FastPath();
                        }
                        catch (Exception) when (Zeroed()) { }
                        catch (Exception e) when (!Zeroed())
                        {
                            _logger.Debug(e, $"Processing protocol failed for {Description}: ");
                        }
                    }
                }

                await batchJob.SetStateAsync(IoJobMeta.JobState.Consumed).FastPath();
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Debug(e, "Error processing message batch");
            }
            finally
            {
                try
                {
                    CcBridgeMessages.Heap.Return(msgBatch);
                }
                catch
                {
                    // ignored
                }
            }
        }

        /// <summary>
        /// Starts the bridge
        /// </summary>
        public static void Start(IIoSource source)
        {
            IoZeroScheduler.Zero.LoadAsyncContext(static async state =>
            {
                var channel = await ((IIoSource)state).CreateConduitOnceAsync<CcProtocBatchJob<chroniton,CcFrameBatch>>(nameof(CcBridgeMessages)).FastPath();
                var network = new CcNetBridge(channel);

                while (!network.Zeroed())
                {
                    try
                    {
                        await network.BridgeAsync().FastPath();
                    }
                    catch when (network.Zeroed())
                    {
                    }
                    catch (Exception e) when (!network.Zeroed())
                    {
                        network._logger.Error(e, $"{nameof(network.BridgeAsync)}: {network.Description}");
                    }

                    await Task.Delay(1000);
                }
            }, source);
        }
    }
}
