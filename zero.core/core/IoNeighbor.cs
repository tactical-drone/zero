using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.data.cassandra;
using zero.core.data.native.cassandra;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled.common.model.interop;
using zero.interop.entangled.common.model.native;
using Logger = NLog.Logger;

namespace zero.core.core
{
    /// <inheritdoc />
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class IoNeighbor<TJob> : IoProducerConsumer<TJob>
    where TJob : IIoWorker
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocMessage">The callback that allocates new message buffer space</param>
        public IoNeighbor(IoNetClient<TJob> ioNetClient, Func<object, IoConsumable<TJob>> mallocMessage)
            : base($"neighbor {ioNetClient.AddressString}", ioNetClient, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();

            Spinners.Token.Register(() => PrimaryProducer?.Close());
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// Called when this neighbor is closed
        /// </summary>
        public event EventHandler Closed;

        /// <summary>
        /// Close this neighbor
        /// </summary>
        public void Close()
        {
            _logger.Info($"Closing neighbor `{PrimaryProducerDescription}'");

            Spinners.Cancel();

            OnClosed();
        }

        /// <summary>
        /// Emits the closed event
        /// </summary>
        public virtual void OnClosed()
        {
            Closed?.Invoke(this, EventArgs.Empty);
        }

        public override async Task SpawnProcessingAsync(CancellationToken cancellationToken, bool spawnProducer = true)
        {
            var processing = base.SpawnProcessingAsync(cancellationToken, spawnProducer);
            var persisting = PersistTransactions();

            processing.Start();
            persisting.Start();

            await Task.WhenAll(processing, persisting);
        }

        private async Task PersistTransactions()
        {
            var relaySource = PrimaryProducer.GetRelaySource<IoTangleTransaction>(nameof(IoNeighbor<IoTangleTransaction>));
            
            _logger.Debug($"Starting persistence for `{PrimaryProducerDescription}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (relaySource == null) //TODO this is hacky 
                {
                    _logger.Warn("Waiting for transaction stream...");
                    relaySource = PrimaryProducer.GetRelaySource<IoTangleTransaction>(nameof(IoNeighbor<IoTangleTransaction>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await relaySource.ConsumeAsync(async batch =>
                {
                    if (batch == null)
                        return;

                    var txBatch = (IoTangleTransaction)batch;

                    foreach (var transaction in txBatch.Transactions)
                    {
                        if (transaction is IoNativeTransactionModel)
                        {
                            await IoNativeCassandra.Default().ContinueWith(session =>
                            {
                                if (session.Result.IsConnected)
                                    session.Result.Put((IoNativeTransactionModel)transaction);
                            });
                        }
                        else
                        {
                            await IoCassandra.Default().ContinueWith(session =>
                            {
                                if (session.Result.IsConnected)
                                    session.Result.Put((IoInteropTransactionModel)transaction);
                            });
                        }                        
                    }                    
                });

            }

            _logger.Debug($"Shutting down persistence for `{PrimaryProducerDescription}'");
        }
    }
}
