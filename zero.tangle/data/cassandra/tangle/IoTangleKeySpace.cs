using Cassandra.Mapping;
using zero.core.feat.data.providers.cassandra;
using zero.tangle.data.cassandra.tangle.luts;
using zero.tangle.models;

namespace zero.tangle.data.cassandra.tangle
{
    /// <summary>
    /// The tangle keyspace config
    /// </summary>
    /// <typeparam name="TKey">key type</typeparam>
    public class IoTangleKeySpace<TKey> : IIoCassandraKeySpace
    {
        /// <summary>
        /// Constructs a new keyspace
        /// </summary>
        /// <param name="name">Keyspace name</param>
        public IoTangleKeySpace(string name)
        {
            Name = name;
        }

        public string Name { get; protected set; }

        private MappingConfiguration _bundle;
        public MappingConfiguration BundleMap
        {
            get
            {
                if (_bundle != null)
                    return _bundle;

                _bundle = new MappingConfiguration();
                _bundle.Define(
                    new Map<IIoTransactionModel<TKey>>().TableName("bundle")
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
                        .Column(c => c.ObsoleteTag)
                        .Column(c => c.AttachmentTimestamp)
                        .Column(c => c.AttachmentTimestampLower)
                        .Column(c => c.AttachmentTimestampUpper)
                        .Column(c => c.MilestoneIndexEstimate)
                        .Column(c => c.Nonce)
                        .Column(c => c.Hash)
                        .Column(c => c.Size)

                        .PartitionKey(c => c.Bundle)
                        .ClusteringKey(c => c.CurrentIndex, SortOrder.Ascending)
                        .ClusteringKey(c => c.Hash, SortOrder.Ascending));                        

                return _bundle;
            }
        }

        public MappingConfiguration BundledAddressMap
        {
            get
            {
                var bundledAddress = new MappingConfiguration();
                bundledAddress.Define(
                    new Map<IoBundledAddress<TKey>>().TableName("address")
                        .ExplicitColumns()
                        .Column(c => c.Address)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)
                        .PartitionKey(c => c.Address)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Descending)
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
                    new Map<IoDraggedTransaction<TKey>>().TableName("dragnet")
                        .ExplicitColumns()                        
                        .Column(c => c.Address)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)
                        .Column(c => c.LocalTimestamp)
                        .Column(c => c.AttachmentTimestamp)                                                
                        .Column(c => c.Value)
                        .Column(c => c.Direction)
                        .Column(c => c.Quality)
                        .Column(c => c.Uri)
                        .Column(c => c.BtcValue)
                        .Column(c => c.EthValue)
                        .Column(c => c.EurValue)
                        .Column(c => c.UsdValue)
                        .PartitionKey(c => c.Address)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Descending)                        
                        .ClusteringKey(c => c.Value, SortOrder.Descending)          
                        .ClusteringKey(c => c.Uri, SortOrder.Ascending)
                        .ClusteringKey(c => c.Bundle, SortOrder.Ascending));
                return draggedTransaction;
            }
        }

        public MappingConfiguration BundledTransactionMap
        {
            get
            {
                var bundledTransaction = new MappingConfiguration();
                bundledTransaction.Define(
                    new Map<IoBundledHash<TKey>>().TableName("transaction")
                        .ExplicitColumns()
                        .Column(c => c.Hash)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)                        
                        .PartitionKey(c => c.Hash));
                return bundledTransaction;
            }
        }

        public MappingConfiguration TaggedTransactionMap
        {
            get
            {
                var taggedTransaction = new MappingConfiguration();
                taggedTransaction.Define(
                    new Map<IoTaggedTransaction<TKey>>().TableName("tag")
                        .ExplicitColumns()
                        .Column(c => c.Tag)
                        .Column(c => c.Partition)
                        .Column(c => c.Hash)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)
                        .PartitionKey(c => c.Partition)
                        .ClusteringKey(c => c.Tag, SortOrder.Ascending)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Descending)                        
                        .ClusteringKey(c => c.Hash, SortOrder.Ascending));
                return taggedTransaction;
            }
        }

        public MappingConfiguration ApprovedTransactionMap
        {
            get
            {
                var verifiedTransaction = new MappingConfiguration();
                verifiedTransaction.Define(
                    new Map<IoApprovedTransaction<TKey>>().TableName("approvee")
                        .ExplicitColumns()
                        .Column(c => c.Partition)
                        .Column(c => c.Hash)                        
                        .Column(c => c.Verifier)
                        .Column(c => c.TrunkBranch)
                        .Column(c => c.Balance)
                        .Column(c => c.Pow)
                        .Column(c => c.Timestamp)
                        .Column(c => c.ConfirmationTime)
                        .Column(c => c.Milestone)
                        .Column(c => c.IsMilestone)                        
                        .Column(c => c.Depth)                        
                        .PartitionKey(c => c.Partition)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Descending)                        
                        .ClusteringKey(c => c.Hash)

                        .ClusteringKey(c => c.Verifier));
                return verifiedTransaction;
            }
        }

        public MappingConfiguration MilestoneTransactionMap
        {
            get
            {
                var milestoneTransaction = new MappingConfiguration();
                milestoneTransaction.Define(
                    new Map<IoMilestoneTransaction<TKey>>().TableName("milestone")
                        .ExplicitColumns()                        
                        .Column(c => c.Partition)
                        .Column(c => c.ObsoleteTag)
                        .Column(c => c.Hash)
                        .Column(c => c.Bundle)
                        .Column(c => c.Timestamp)                        
                        .Column(c => c.MilestoneIndex)
                        .PartitionKey(c => c.Partition)
                        .ClusteringKey(c => c.Timestamp, SortOrder.Descending)
                        .ClusteringKey(c => c.MilestoneIndex, SortOrder.Descending)                        
                        .ClusteringKey(c => c.Hash, SortOrder.Ascending));
                return milestoneTransaction;
            }
        }
    }
}
