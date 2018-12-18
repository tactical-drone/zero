﻿using System;
using System.Threading.Tasks;
using Cassandra;
using NLog;
using zero.core.network.ip;
using Logger = NLog.Logger;

namespace zero.core.data.cassandra
{
    public abstract class IoCassandraBase<TBlob> : IoCassandraKeyBase<TBlob> 
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

        protected volatile bool _connecting = false;
        private bool _isConnected;

        public bool IsConnected
        {
            get => _isConnected;
            protected set
            {
                //Quick retry
                if (value == false && _isConnected)
                    _cluster = null;                

                _isConnected = value;
            }
        }
        protected string _dbUrl = string.Empty;

        protected async Task<bool> Connect(string url)
        {
            if (_connecting)
                return false;

            _connecting = true;

            if (_cluster != null)
            {
                if(!_connecting)
                    _logger.Warn($"Cluster was connected at `{_clusterAddress.IpEndPoint}', attempting to reconnect");
                else
                    _logger.Warn($"Cluster at `{_clusterAddress.IpEndPoint}', attempting to reconnect");

                await Task.Delay(30000);//TODO config
            }
            
            _dbUrl = url;

            _clusterAddress = IoNodeAddress.Create(url);
            _cluster = Cluster.Builder().AddContactPoint(_clusterAddress.IpEndPoint).Build();

            _logger.Debug("Connecting to Cassandra...");

            try
            {
                _session = await _cluster.ConnectAsync();
            }
            catch (Exception e)
            {
                _cluster = null;                
                _logger.Error(e, $"Unable to connect to cassandra database `{_clusterAddress.UrlAndPort}` at `{_clusterAddress.ResolvedIpAndPort}':");
                
                return _connecting = false;
            }
            
            _logger.Debug($"Connected to Cassandra cluster = `{_cluster.Metadata.ClusterName}'");

            if (!EnsureSchema())
                _logger.Info("Configured db schema");

            IsConnected = true;
            _connecting = false;

            return true;
        }

        protected abstract bool EnsureSchema();

        public async Task<RowSet> ExecuteAsync(BatchStatement batch)
        {
            var executeAsyncTask = _session.ExecuteAsync(batch);
#pragma warning disable 4014
            executeAsyncTask.ContinueWith(r =>
#pragma warning restore 4014
            {
                switch (r.Status)
                {
                    case TaskStatus.Canceled:
                    case TaskStatus.Faulted:
                        _logger.Error(r.Exception, "Put data returned with errors:");
                        break;
                }
            });
            return await executeAsyncTask;
        }
    }
}
