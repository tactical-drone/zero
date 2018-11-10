using System;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Security.Cryptography;
using System.Threading;
using NLog;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.misc;

namespace zero.core.core
{
    /// <summary>
    /// Represents a node's neighbor
    /// </summary>
    public class Neighbor : IoMessageHandler<IoNetClient>
    {
        /// <summary>
        /// Construct
        /// </summary>
        /// <param name="ioNetClient">The neighbor rawSocket wrapper</param>
        public Neighbor(IoNetClient ioNetClient) : base($"neighbor {ioNetClient.Address}", 
        () => new IoP2Message(ioNetClient) {JobDescription = $"rx", WorkDescription = $"{ioNetClient.Address}" })
        {
            _logger = LogManager.GetCurrentClassLogger();
            IoNetClient = ioNetClient;

            Spinners.Token.Register(() => IoNetClient?.Close());
        }
        
        /// <summary>
        /// logger
        /// </summary>
        private readonly Logger _logger;

        public event EventHandler Closed;

        #region properties
        
        private IoNetClient _ioNetClient;
        /// <summary>
        /// The client wrapper porperty, sets up <see cref="IoMessageHandler{IoNetClient}.StreamDescriptor"/> to <see cref="network.ip.IoNetClient.Address"/>
        /// </summary>
        public IoNetClient IoNetClient
        {
            get => _ioNetClient;
            set
            {
                _ioNetClient = value;
                StreamDescriptor = _ioNetClient.Address;
            }
        }
        #endregion

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            _logger.Info($"Closing neighbor `{Description}'");

            Spinners.Cancel();            

            OnClosed();
        }

        protected virtual void OnClosed()
        {
            Closed?.Invoke(this, EventArgs.Empty);
        }
    }
}
