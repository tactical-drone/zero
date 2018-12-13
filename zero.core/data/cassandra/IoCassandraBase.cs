﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Mapping;
using NLog;
using zero.core.network.ip;
using Logger = NLog.Logger;

namespace zero.core.data.cassandra
{
    public abstract class IoCassandraBase
    {
        protected IoCassandraBase()
        {
            _logger = LogManager.GetCurrentClassLogger();           
        }

        private readonly Logger _logger;

        protected string Keyspace;
        protected Cluster _cluster;
        protected ISession _session;        
        protected IoNodeAddress _clusterAddress;

        public bool IsConnected = false;

        protected async Task<bool> Connect(string url)
        {
            if (_cluster != null)
            {
                _logger.Error($"Cluster already connected at {_clusterAddress.IpEndPoint}");
                return false;
            }

            _clusterAddress = IoNodeAddress.Create(url);            
            _cluster = Cluster.Builder().AddContactPoint(_clusterAddress.IpEndPoint).Build();
            
            _logger.Debug("Connecting to Cassandra...");

            try
            {
                _session = await _cluster.ConnectAsync();
            }
            catch (Exception e)
            {
                _logger.Error(e,$"Unable to connect to cassandra database `{_clusterAddress.UrlAndPort}` at `{_clusterAddress.ResolvedIpAndPort}':");
                return false;
            }

            IsConnected = true;
            
            _logger.Debug($"Connected to Cassandra cluster = `{_cluster.Metadata.ClusterName}'");            

            if (!EnsureSchema())
                _logger.Info("Configured db schema");
                
            return true;
        }

        protected abstract bool EnsureSchema();
    }
}
