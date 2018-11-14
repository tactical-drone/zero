using System;
using System.Text;
using NLog;
using Tangle.Net.Cryptography;
using Tangle.Net.Entity;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using zero.core.patterns.schedulers;

namespace zero.core.protocol
{
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer : IoNeighbor
    {
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient ioNetClient) :
            base(ioNetClient, () => new IoTangleMessage(ioNetClient) { JobDescription = $"rx", WorkDescription = $"{ioNetClient.Address}" })
        {
            _logger = LogManager.GetCurrentClassLogger();

            JobThreadScheduler = new LimitedThreadScheduler(parm_max_consumer_threads = 4);
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;        

        /// <summary>
        /// Hack to disregard the port data sent by iri when it connects
        /// </summary>
        private bool _portDataDisgarded = false;

        /// <summary>
        /// Does work on the messages received
        /// </summary>
        /// <param name="currJob">The current job fragment to be procesed</param>
        /// <param name="previousJobFragment">Include a previous job fragment if that job had a fragmented datum</param>
        /// <returns>The state of work done</returns>
        protected override IoProducable<IoNetClient>.State Consume(IoMessage<IoNetClient> currJob, IoMessage<IoNetClient> previousJobFragment = null)
        {
            var tangleMessage = (IoTangleMessage)currJob;
            IoProducable<IoNetClient>.State produceState;
            if ((produceState = base.Consume(tangleMessage)) >= IoProducable<IoNetClient>.State.Error)
                return produceState;

            //TODO Find a more elegant way for this terrible hack
            //Disgard the neighbor port data
            if (!_portDataDisgarded)
            {
                _portDataDisgarded = true;
                _logger.Trace($"Got receiver port as: `{Encoding.ASCII.GetString((byte[])(Array)tangleMessage.Buffer).Substring(tangleMessage.BufferOffset, 10)}'");
                tangleMessage.BufferOffset += 10;
                if (tangleMessage.BytesLeftToProcess == 0)
                    return currJob.ProcessState = IoProducable<IoNetClient>.State.Consumed;
            }

            //Process protocol messages
            ProcessProtocolMessage(tangleMessage);

            //_logger.Info($"Processed `{message.DatumCount}' datums, remainder = `{message.DatumFragmentLength}', message.BytesRead = `{message.BytesRead}'," +
            //             $" prevJob.BytesLeftToProcess =`{previousJobFragment?.BytesLeftToProcess}'");
            return currJob.ProcessState;
        }

        /// <summary>
        /// Processes a protocol message broadcasted by another peer
        /// </summary>
        /// <param name="message">The job structure containing the next message to be processed</param>
        private void ProcessProtocolMessage(IoTangleMessage message)
        {
            for (int i = 0; i < message.DatumCount; i++)
            {
                ternary.Codec.GetTrits(message.Buffer, message.BufferOffset, message.TritBuffer, IoTangleMessage.TransactionSize);
                var trytes = Converter.TritsToTrytes(message.TritBuffer);

                var tx = Transaction.FromTrytes(new TransactionTrytes(trytes));
                //if (tx.Value != 0 && tx.Value < 9999999999999999 && tx.Value > -9999999999999999)
                    _logger.Info($"addr = {tx.Address}, value = {(tx.Value / 1000000).ToString().PadLeft(17,' ')} Mi, f = {message.DatumFragmentLength != 0}");

                message.BufferOffset += IoTangleMessage.DatumLength;
            }

            message.ProcessState = message.DatumFragmentLength != 0 ? IoProducable<IoNetClient>.State.ConsumerFragmented : IoProducable<IoNetClient>.State.Consumed;
        }
    }
}
