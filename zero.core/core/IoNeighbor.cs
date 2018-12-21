using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using zero.core.data.contracts;
using zero.core.data.providers.cassandra;
using zero.core.models.consumables;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.interop.entangled;
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
        /// <param name="kind">A description of the neighbor</param>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        /// <param name="mallocMessage">The callback that allocates new message buffer space</param>
        public IoNeighbor(string kind, IoNetClient<TJob> ioNetClient, Func<object, IoConsumable<TJob>> mallocMessage)
            : base($"{kind} `{ioNetClient.Description}'", ioNetClient, mallocMessage)
        {
            _logger = LogManager.GetCurrentClassLogger();

            Spinners.Token.Register(() => PrimaryProducer?.Close());
        }

        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        private bool _closed = false;

        /// <summary>
        /// Called when this neighbor is closed
        /// </summary>
        public event EventHandler Closed;

        /// <summary>
        /// Close this neighbor
        /// </summary>
        public void Close()
        {
            if(_closed) return;
            _closed = true;

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
            Task persisting = null;

            persisting = IoEntangled<object>.Optimized ? PersistTransactions<byte[]>(await IoCassandra<byte[]>.Default()) : PersistTransactions<string>(await IoCassandra<string>.Default());            

            await Task.WhenAll(processing, persisting);
        }



        private async Task PersistTransactions<TBlob>(IIoDataSource<TBlob> dataSource) 
        {
            var relaySource = PrimaryProducer.GetRelaySource<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));                       
            
            _logger.Debug($"Starting persistence for `{PrimaryProducerDescription}'");
            while (!Spinners.IsCancellationRequested)
            {
                if (relaySource == null)
                {
                    _logger.Warn("Waiting for transaction stream to spin up...");
                    relaySource = PrimaryProducer.GetRelaySource<IoTangleTransaction<TBlob>>(nameof(IoNeighbor<IoTangleTransaction<TBlob>>));
                    await Task.Delay(2000);//TODO config
                    continue;
                }

                await relaySource.ConsumeAsync(async batch =>
                {
                    try
                    {
                        if (batch == null)
                            return;

                        foreach (var transaction in ((IoTangleTransaction<TBlob>) batch).Transactions)
                        {                            
                            var rows = await dataSource.Put(transaction);
                            if (rows == null)
                                batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.ConInvalid;
                        }
                        batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.Consumed;
                    }
                    finally
                    {
                        if (batch != null && batch.ProcessState == IoProduceble<IoTangleTransaction<TBlob>>.State.Consuming)
                            batch.ProcessState = IoProduceble<IoTangleTransaction<TBlob>>.State.ConsumeErr;
                    }                    
                });

                if (!relaySource.PrimaryProducer.IsOperational)
                    break;
            }

            _logger.Debug($"Shutting down persistence for `{PrimaryProducerDescription}'");
        }        
    }
}
