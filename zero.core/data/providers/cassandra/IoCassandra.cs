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
using zero.core.network.ip;
using zero.interop.entangled;
using zero.interop.entangled.common.model.interop;
using Logger = NLog.Logger;

namespace zero.core.data.providers.cassandra
{
    /// <summary>
    /// Cassandra storage provider
    /// </summary>
    /// <typeparam name="TBlob"></typeparam>
    public class IoCassandra<TBlob> : IoCassandraBase<TBlob>, IIoDataSource<RowSet> 
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public IoCassandra()
        {
            _logger = LogManager.GetCurrentClassLogger();
            Keyspace = IoEntangled<TBlob>.Optimized ? "zero" : "one";
        }

        private readonly Logger _logger;
        
        private Table<IIoTransactionModel<TBlob>> _transactions;
        private Table<IoBundledHash<TBlob>> _hashes;
        private Table<IoBundledAddress<TBlob>> _addresses;
        private Table<IoTaggedTransaction<TBlob>> _tags;
        private Table<IoVerifiedTransaction<TBlob>> _verifiers;
        private Table<IoDraggedTransaction<TBlob>> _dragnet;

        /// <summary>
        /// Makes sure that the schema is configured
        /// </summary>
        /// <returns>True if the schema was re-configured, false otherwise</returns>
        protected override async Task<bool> EnsureSchema()
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
                KeyspaceMetadata keyspace = _cluster.Metadata.GetKeyspace(Keyspace);

                while (keyspace == null) 
                {
                    keyspace = _cluster.Metadata.GetKeyspace(Keyspace);
                    await Task.Delay(1000);//TODO config
                }

                var existingTables = keyspace.GetTablesNames();

                _transactions = new Table<IIoTransactionModel<TBlob>>(_session, new IoCassandraKeyBase<TBlob>().BundleMap);
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

        /// <summary>
        /// Puts data into cassandra
        /// </summary>
        /// <param name="transaction">The transaction to persist</param>
        /// <param name="batch">A batch handler</param>
        /// <returns>The rowset with insert results</returns>
        public async Task<RowSet> Put<TBlobLocal>(IIoTransactionModel<TBlobLocal> transaction, object batch = null)        
        {
            if (!IsConnected)
            {
#pragma warning disable 4014
                Connect(_clusterAddress);
#pragma warning restore 4014
                return null;
            }
            
            if (transaction.HashBuffer.Length == 0)
            {
                try
                {
                    _logger.Trace($"Invalid transaction");
                    _logger.Trace($"value = `{transaction.Value}'");
                    _logger.Trace($"pow = `{transaction.Pow}'");
                    _logger.Trace($"time = `{transaction.Timestamp}'");
                    _logger.Trace($"bundle = `{transaction.AsTrytes(transaction.BundleBuffer)}'");
                    _logger.Trace($"address = `{transaction.AsTrytes(transaction.AddressBuffer)}'");
                }
                catch {}

                return null;
            }
                                        
            var executeBatch = batch == null;

            var bundledHash = new IoBundledHash<TBlobLocal>
            {
                Hash = transaction.Hash,
                Bundle = transaction.Bundle,
                Timestamp = transaction.Timestamp                                
            };

            var bundledAddress = new IoBundledAddress<TBlobLocal>
            {
                Address = transaction.Address,
                Bundle = transaction.Bundle,
                Timestamp = transaction.Timestamp
            };
            
            var taggedTransaction = new IoTaggedTransaction<TBlobLocal>
            {
                Tag = transaction.Tag,
                ObsoleteTag = transaction.ObsoleteTag,
                Hash = transaction.Hash,
                Timestamp = transaction.Timestamp
            };

            var verifiedBranchTransaction = new IoVerifiedTransaction<TBlobLocal>
            {
                Hash = transaction.Branch,
                Pow = transaction.Pow,
                Verifier = transaction.Hash,
                Timestamp = transaction.Timestamp
            };

            var verifiedTrunkTransaction = new IoVerifiedTransaction<TBlobLocal>
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
                    double quality;
                    try
                    {
                        quality = IoMarketDataClient.Quality + (DateTime.Now - (transaction.Timestamp.ToString().Length > 11? DateTimeOffset.FromUnixTimeMilliseconds(transaction.Timestamp): DateTimeOffset.FromUnixTimeSeconds(transaction.Timestamp))).TotalMinutes;

                        if (quality > short.MaxValue)
                            quality = short.MaxValue;
                        if (quality < short.MinValue)
                            quality = short.MinValue;
                    }
                    catch 
                    {
                        quality = short.MaxValue;
                    }

                    var draggedTransaction = new IoDraggedTransaction<TBlobLocal>
                    {
                        Address = transaction.Address,
                        Bundle = transaction.Bundle,
                        AttachmentTimestamp = transaction.AttachmentTimestamp,
                        Timestamp = transaction.Timestamp,
                        LocalTimestamp = ((DateTimeOffset)DateTime.Now).ToUnixTimeSeconds(),
                        Value = transaction.Value,
                        Quality = (short)quality,
                        Uri = transaction.Uri,                                                
                        BtcValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Btc.Price / IoMarketDataClient.BundleSize)),
                        EthValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eth.Price / IoMarketDataClient.BundleSize)),
                        EurValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Eur.Price / IoMarketDataClient.BundleSize)),
                        UsdValue = (float)(transaction.Value * (IoMarketDataClient.CurrentData.Raw.Iot.Usd.Price / IoMarketDataClient.BundleSize))
                    };
                    
                    ((BatchStatement)batch).Add(_dragnet.Insert(draggedTransaction as IoDraggedTransaction<TBlob>));
                }
                catch (Exception e)
                {
                    _logger.Warn(e, "Unable to drag transaction:");
                }                
            }

            ((BatchStatement)batch).Add(_transactions.Insert((IIoTransactionModel<TBlob>) transaction));
            ((BatchStatement)batch).Add(_hashes.Insert(bundledHash as IoBundledHash<TBlob>));
            
            // ReSharper disable once PossibleNullReferenceException
            //if (transaction.Branch != null && ((transaction.Branch is string branch ? branch.Length : (transaction.Branch as byte[]).Length) > 0))
            if(transaction.BranchBuffer.Length != 0)
                ((BatchStatement)batch).Add(_verifiers.Insert(verifiedBranchTransaction as IoVerifiedTransaction<TBlob>));

            // ReSharper disable once PossibleNullReferenceException
            if (transaction.TrunkBuffer.Length != 0)
                ((BatchStatement)batch).Add(_verifiers.Insert(verifiedTrunkTransaction as IoVerifiedTransaction<TBlob>));

            // ReSharper disable once PossibleNullReferenceException
            if (transaction.AddressBuffer.Length != 0)
                ((BatchStatement)batch).Add(_addresses.Insert(bundledAddress as IoBundledAddress<TBlob>));

            // ReSharper disable once PossibleNullReferenceException
            if (transaction.TagBuffer.Length != 0)
                ((BatchStatement)batch).Add(_tags.Insert(taggedTransaction as IoTaggedTransaction<TBlob>));
                        
            if (executeBatch)
            {
                try
                {
                    return await ExecuteAsync((BatchStatement)batch);
                }
                catch (Exception e)
                {
                    _logger.Trace(e, "An error has occurred inserting row:");

                    try
                    {
                        _logger.Trace($"value = `{transaction.Value}'");
                        _logger.Trace($"pow = `{transaction.Pow}'");
                        _logger.Trace($"time = `{transaction.Timestamp}'");
                        _logger.Trace($"bundle = `{transaction.AsTrytes(transaction.BundleBuffer)}'");
                        _logger.Trace($"address = `{transaction.AsTrytes(transaction.AddressBuffer)}'");
                    }
                    catch
                    {
                        // ignored
                    }

                    IsConnected = false;
                }
            }

            return null;
        }

        /// <summary>
        /// Get a transaction from storage
        /// </summary>
        /// <param name="key">The transaction key</param>
        /// <returns>A transaction</returns>
        public Task<IIoTransactionModel<TBlob>> Get<TBlob>(ReadOnlyMemory<byte> key)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Execute a batch
        /// </summary>
        /// <param name="batch">The batch handler</param>
        /// <returns>The rowset with execution results</returns>
        public async Task<RowSet> ExecuteAsync(object batch)
        {
            return await base.ExecuteAsync((BatchStatement)batch);
        }

        
        private static volatile IoCassandra<TBlob> _default;

        /// <summary>
        /// Returns single connection
        /// </summary>
        /// <returns></returns>
        public static async Task<IIoDataSource<RowSet>> Default()
        {
            if (_default != null) return _default;
            
            _default = new IoCassandra<TBlob>();
            await _default.Connect(IoNodeAddress.Create("tcp://10.0.75.1:9042")); //TODO config
            return _default;            
        }
    }
}
