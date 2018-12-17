using System;
using System.Collections.Generic;
using System.Text;
using Cassandra.Mapping;
using zero.core.data.lookups;
using zero.interop.entangled.common.model.interop;

namespace zero.core.data.cassandra
{
    public class IoCassandraSchemaBase
    {
        private MappingConfiguration _bundle;
        public MappingConfiguration Bundle
        {
            get
            {
                if (_bundle != null)
                    return _bundle;

                _bundle = new MappingConfiguration();
                _bundle.Define(
                new Map<IoMarshalledTransaction>().TableName("bundle")
                    .ExplicitColumns()
                    .Column(c => c.signature_or_message)
                    .Column(c => c.address)
                    .Column(c => c.value)
                    .Column(c => c.timestamp)
                    .Column(c => c.current_index)
                    .Column(c => c.last_index)                    
                    .Column(c => c.bundle, map => map.AsFrozen())
                    .Column(c => c.trunk)
                    .Column(c => c.branch)
                    .Column(c => c.tag, map => map.AsFrozen())
                    .Column(c => c.attachment_timestamp)
                    .Column(c => c.attachment_timestamp_lower)
                    .Column(c => c.attachment_timestamp_upper)
                    .Column(c => c.nonce)
                    .Column(c => c.hash)
                    .Column(c => c.Size)

                    .PartitionKey(c => c.bundle, c => c.last_index)
                    .ClusteringKey(c => c.current_index, SortOrder.Ascending));
                return _bundle;
            }
        }

        private  MappingConfiguration _dragnet;
        public  MappingConfiguration Dragnet
        {
            get
            {
                if (_dragnet != null)
                    return _dragnet;

                _dragnet = new MappingConfiguration();
                _dragnet.Define(
                    new Map<IoDraggedTransaction>().TableName("dragnet")
                        .ExplicitColumns()
                        .Column(c => c.Address)                        

                        .PartitionKey(c => c.Address, c => c.CounterParty)
                        .ClusteringKey(c => c.timestamp, SortOrder.Descending));
                return _dragnet;
            }
        }
    }
}
