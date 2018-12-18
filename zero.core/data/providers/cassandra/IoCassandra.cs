using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using NLog;
using zero.core.data.cassandra;
using zero.core.data.contracts;
using zero.core.data.lookups;
using zero.core.data.luts;
using zero.core.data.market;
using zero.interop.entangled;
using zero.interop.entangled.common.model;
using zero.interop.entangled.common.model.interop;
using Logger = NLog.Logger;

namespace zero.core.data.providers.cassandra
{
    public class IoCassandra<TBlob>: IoCassandraBase<TBlob>, IIoDataSource<TBlob> 
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoCassandra()
        {
            _logger = LogManager.GetCurrentClassLogger();
            Keyspace = IoEntangled<object>.Optimized ? "zero" : "one";
        }

        private readonly Logger _logger;
        
        private Table<IIoInteropTransactionModel<TBlob>> _transactions;
        private Table<IoBundledHash<TBlob>> _hashes;
        private Table<IoBundledAddress<TBlob>> _addresses;
        private Table<IoTaggedTransaction<TBlob>> _tags;
        private Table<IoVerifiedTransaction<TBlob>> _verifiers;
        private Table<IoDraggedTransaction<TBlob>> _dragnet;

        protected override bool EnsureSchema()
        {   
            _logger.Debug("Ensuring db schema...");
            bool wasConfigured = true;

            //ensure keyspace
            var replicationConfig = new Dictionary<string, string>
            {
                {"class", "SimpleStrategy"}, {"replication_factor", "1"}
            };

            _session.CreateKeyspaceIfNotExists(Keyspace, replicationConfig, false);
            _session.ChangeKeyspace(Keyspace);
            
            //ensure tables            
            try
            {                                
                var existingTables = _cluster.Metadata.GetKeyspace(Keyspace).GetTablesNames();

                _transactions = new Table<IIoInteropTransactionModel<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().BundleMap);
                if (!existingTables.Contains("bundle"))
                {
                    _transactions.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_transactions.Name}'");
                    wasConfigured = false;
                }
                                                                    
                _hashes = new Table<IoBundledHash<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().BundledTransaction);
                if (!existingTables.Contains("transaction"))
                {
                    _hashes.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_hashes.Name}'");
                    wasConfigured = false;
                }
                    
                _addresses = new Table<IoBundledAddress<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().BundledAddressMap);
                if (!existingTables.Contains("address"))
                {
                    _addresses.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_addresses.Name}'");
                    wasConfigured = false;
                }

                _tags = new Table<IoTaggedTransaction<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().TaggedTransaction);
                if (!existingTables.Contains("tag"))
                {
                    _tags.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_tags.Name}'");
                    wasConfigured = false;
                }

                _verifiers = new Table<IoVerifiedTransaction<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().VerifiedTransaction);
                if (!existingTables.Contains("verifier"))
                {
                    _verifiers.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_verifiers.Name}'");
                    wasConfigured = false;
                }

                _dragnet = new Table<IoDraggedTransaction<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().DraggedTransactionMap);
                if (!existingTables.Contains("dragnet"))
                {
                    _dragnet.CreateIfNotExists();
                    _logger.Debug($"Adding table `{_dragnet.Name}'");
                    wasConfigured = false;
                }
            }
            catch (Exception e)
            {
                _logger.Error(e,"Unable to ensure schema:");
                return true;                
            }

            _logger.Trace("Ensured schema!");
            base.IsConnected = true;
            return wasConfigured;
        }        

        public async Task<RowSet> Put(IIoInteropTransactionModel<TBlob> transaction, object batch = null)
        {
            if (!IsConnected)
            {
                await Connect(_dbUrl);
                return null;
            }

            //check for pow
            if (transaction.Pow == 0)
                return null;

            if (transaction.Hash == null || (transaction.Hash is string
                    ? (transaction.Hash as string).Length
                    // ReSharper disable once PossibleNullReferenceException
                    : (transaction.Hash as byte[]).Length) == 0)
            {
                try
                {
                    _logger.Trace($"Invalid transaction");
                    _logger.Trace($"Transaction = pow = `{transaction.Pow}'");
                    _logger.Trace($"`{transaction.AsTrytes(transaction.Bundle, IoTransaction.NUM_TRITS_BUNDLE, IoTransaction.NUM_TRITS_BUNDLE)}'");
                    _logger.Trace($"address = '{transaction.AsTrytes(transaction.Address, IoTransaction.NUM_TRITS_ADDRESS, IoTransaction.NUM_TRITS_ADDRESS)}'");
                }
                catch {}

                return null;
            }
                                        
            var executeBatch = batch == null;

            var bundledHash = new IoBundledHash<TBlob>
            {
                Hash = transaction.Hash,
                Bundle = transaction.Bundle,
                Timestamp = transaction.Timestamp                                
            };

            var bundledAddress = new IoBundledAddress<TBlob>
            {
                Address = transaction.Address,
                Bundle = transaction.Bundle,
                Timestamp = transaction.Timestamp
            };
            
            var taggedTransaction = new IoTaggedTransaction<TBlob>
            {
                Tag = transaction.Tag,
                ObsoleteTag = transaction.ObsoleteTag,
                Hash = transaction.Hash,
                Timestamp = transaction.Timestamp
            };

            var verifiedBranchTransaction = new IoVerifiedTransaction<TBlob>
            {
                Hash = transaction.Branch,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,
                Timestamp = transaction.Timestamp
            };

            var verifiedTrunkTransaction = new IoVerifiedTransaction<TBlob>
            {
                Hash = transaction.Trunk,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,
                Timestamp = transaction.Timestamp
            };

            if (executeBatch)
                batch = new BatchStatement();

            if (transaction.Value != 0)
            {                
                try
                {
                    var draggedTransaction = new IoDraggedTransaction<TBlob>
                    {
                        Address = transaction.Address,
                        Bundle = transaction.Bundle,
                        AttachmentTimestamp = transaction.AttachmentTimestamp,
                        Timestamp = transaction.Timestamp,
                        Value = transaction.Value,
                        Quality = IoMarketDataClient.Quality,
                        Uri = transaction.Uri,                                                
                        BtcValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Btc.Price / IoMarketDataClient.BundleSize)),
                        EthValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eth.Price / IoMarketDataClient.BundleSize)),
                        EurValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eur.Price / IoMarketDataClient.BundleSize)),
                        UsdValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Usd.Price / IoMarketDataClient.BundleSize))
                    };
                    
                    ((BatchStatement)batch).Add(_dragnet.Insert(draggedTransaction));
                }
                catch (Exception e)
                {
                    _logger.Warn(e, "Unable to drag transaction:");
                }                
            }

            ((BatchStatement)batch).Add(_transactions.Insert(transaction));
            ((BatchStatement)batch).Add(_hashes.Insert(bundledHash));
            ((BatchStatement)batch).Add(_verifiers.Insert(verifiedBranchTransaction));
            ((BatchStatement)batch).Add(_verifiers.Insert(verifiedTrunkTransaction));

            // ReSharper disable once PossibleNullReferenceException
            if ((transaction.Address is string ? (transaction.Address as string).Length : (transaction.Address as byte[]).Length) > 0)
                ((BatchStatement)batch).Add(_addresses.Insert(bundledAddress));            

            // ReSharper disable once PossibleNullReferenceException
            if((transaction.Tag is string? (transaction.Tag as string).Length : (transaction.Tag as byte[]).Length) > 0) ((BatchStatement)batch).Add(_tags.Insert(taggedTransaction));
                        
            if (executeBatch)
            {
                try
                {
                    await ExecuteAsync((BatchStatement)batch);
                }
                catch (Exception e)
                {
                    _logger.Trace(e, "An error has occurred inserting row:");

                    try
                    {
                        _logger.Trace($"Transaction = pow = `{transaction.Pow}'");
                        _logger.Trace(
                            $"`{transaction.AsTrytes(transaction.Bundle, IoTransaction.NUM_TRITS_BUNDLE, IoTransaction.NUM_TRITS_BUNDLE)}'");
                        _logger.Trace(
                            $"address = '{transaction.AsTrytes(transaction.Address, IoTransaction.NUM_TRITS_ADDRESS, IoTransaction.NUM_TRITS_ADDRESS)}'");
                    }
                    catch{}                                        
                    
                    IsConnected = false;
                }
            }

            return null;
        }

        public Task<IoInteropTransactionModel> Get(TBlob key)
        {
            throw new NotImplementedException();
        }

        public async Task<RowSet> ExecuteAsync(object batch)
        {
            return await base.ExecuteAsync((BatchStatement)batch);
        }

        private static volatile IoCassandra<TBlob> _default;
        public static async Task<IIoDataSource<TBlob>> Default()
        {
            if (_default != null) return _default;
            
            _default = new IoCassandra<TBlob>();
            await _default.Connect("tcp://10.0.75.1:9042");
            return _default;            
        }
    }
}
