using Cassandra.Mapping;
using zero.core.data.lookups;
using zero.core.data.luts;
using zero.interop.entangled.common.model.interop;

namespace zero.core.data.cassandra
{
    /// <summary>
    /// The keystore config
    /// </summary>
    /// <typeparam name="TBlob">Blob type</typeparam>
    public class IoCassandraKeyBase 
    {
        private MappingConfiguration _bundle;
        public MappingConfiguration BundleMap
        {
            get
            {
                if (_bundle != null)
                    return _bundle;

                _bundle = new MappingConfiguration();
                _bundle.Define(
                    new Map<IIoTransactionModel>().TableName("bundle")
                        .ExplicitColumns()
                        .Column(c => c.SignatureOrMessage)
                        .Column(c => c.Address)
                        .Column(c => c.Value)
                        .Column(c => c.Timestamp)
                        .Column(c => c.CurrentIndex)
                        .Column(c => c.LastIndex)
                        .Column(c => c.Bundle, map =>
                        {
                            //if (IoEntangled<object>.Optimized)
                            //    map.AsFrozen();
                        })
                        .Column(c => c.Trunk)
                        .Column(c => c.Branch)
                        .Column(c => c.Tag, map =>
                        {
                            //if (IoEntangled<object>.Optimized)
                            //    map.AsFrozen();
                        })
                        .Column(c => c.AttachmentTimestamp)
                        .Column(c => c.AttachmentTimestampLower)
                        .Column(c => c.AttachmentTimestampUpper)
                        .Column(c => c.Nonce)
                        .Column(c => c.Hash)
                        .Column(c => c.Size)

                        .PartitionKey(c => c.Bundle)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Ascending)
                        .ClusteringKey(c => c.CurrentIndex, SortOrder.Ascending));

                return _bundle;
            }
        }

        public MappingConfiguration BundledAddressMap
        {
            get
            {
                var bundledAddress = new MappingConfiguration();
                bundledAddress.Define(
                    new Map<IoBundledAddress>().TableName("address")
                        .ExplicitColumns()
                        .Column(c => c.Address)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)
                        .PartitionKey(c => c.Address)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Ascending)
                        .ClusteringKey(c => c.Bundle));
                return bundledAddress;
            }
        }

        public MappingConfiguration DraggedTransactionMap
        {
            get
            {
                var draggedTransaction = new MappingConfiguration();
                draggedTransaction.Define(
                    new Map<IoDraggedTransaction>().TableName("dragnet")
                        .ExplicitColumns()                        
                        .Column(c => c.Address)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)
                        .Column(c => c.LocalTimestamp)
                        .Column(c => c.AttachmentTimestamp)                                                
                        .Column(c => c.Value)
                        .Column(c => c.Quality)
                        .Column(c => c.Uri)
                        .Column(c => c.BtcValue)
                        .Column(c => c.EthValue)
                        .Column(c => c.EurValue)
                        .Column(c => c.UsdValue)
                        .PartitionKey(c => c.Address)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Ascending)
                        .ClusteringKey(c => c.Value, SortOrder.Descending)                        
                        .ClusteringKey(c => c.Uri, SortOrder.Ascending)
                        .ClusteringKey(c => c.Bundle, SortOrder.Ascending));
                return draggedTransaction;
            }
        }

        public MappingConfiguration BundledTransaction
        {
            get
            {
                var bundledTransaction = new MappingConfiguration();
                bundledTransaction.Define(
                    new Map<IoBundledHash>().TableName("transactions")
                        .ExplicitColumns()
                        .Column(c => c.Hash)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)
                        .PartitionKey(c => c.Hash)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Ascending)
                        .ClusteringKey(c => c.Bundle, SortOrder.Ascending));
                return bundledTransaction;
            }
        }

        public MappingConfiguration TaggedTransaction
        {
            get
            {
                var taggedTransaction = new MappingConfiguration();
                taggedTransaction.Define(
                    new Map<IoTaggedTransaction>().TableName("tag")
                        .ExplicitColumns()
                        .Column(c => c.Tag)
                        .Column(c => c.ObsoleteTag)
                        .Column(c => c.Hash)
                        .Column(c => c.Timestamp)
                        .PartitionKey(c => c.Tag)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Ascending)
                        .ClusteringKey(c => c.Hash, SortOrder.Ascending));
                return taggedTransaction;
            }
        }

        public MappingConfiguration VerifiedTransaction
        {
            get
            {
                var verifiedTransaction = new MappingConfiguration();
                verifiedTransaction.Define(
                    new Map<IoVerifiedTransaction>().TableName("verifier")
                        .ExplicitColumns()
                        .Column(c => c.Hash)
                        .Column(c=> c.Timestamp)
                        .Column(c => c.Verifier)
                        .Column(c => c.Pow)
                        .PartitionKey(c => c.Hash)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Ascending));
                return verifiedTransaction;
            }
        }
    }
}
