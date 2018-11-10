using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using zero.core.patterns.misc;
using NLog;
using Org.BouncyCastle.Bcpg;
using zero.core.conf;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;
using Console = System.Console;

namespace zero.core.protocol
{
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer :Neighbor
    {                                
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient ioNetClient): base(ioNetClient)
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The number of IOTA protocol messages have been received
        /// </summary>
        private long _tangleMessageCount = 0;

        /// <summary>
        /// The current size of tangle protocol messages
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_tangle_msg_size = 1650;

        /// <summary>
        /// The current size of tangle protocol messages
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public long parm_tangle_crc_size = 16;

        /// <summary>
        /// The IOTA protocol message size
        /// </summary>
        public static long TangleDatumSize = 1650 + 16;

        /// <summary>
        /// Does work on the messages received
        /// </summary>
        /// <param name="currFragment">The current job fragment to be procesed</param>
        /// <param name="prevFragment">Include a previous job fragment if job spans two productions</param>
        /// <returns>The state of work done</returns>
        protected override IoProducable<IoNetClient>.State Consume(IoMessage<IoNetClient> currFragment, IoMessage<IoNetClient> prevFragment = null)
        {
            IoProducable<IoNetClient>.State produceState;
            if ((produceState = base.Consume(currFragment)) >= IoProducable<IoNetClient>.State.Error)
                return produceState;

            //TODO Find a more elegant way
            if (TotalBytesReceived == 10)
            {
                _logger.Trace($"Got receiver port as: `{Encoding.ASCII.GetString(currFragment.Buffer).Substring(0,10)}'");
                return currFragment.ProcessState = IoProducable<IoNetClient>.State.Consumed;
            }

            //Calculate how many complete protocol messages we have received   
            var totalBytesAvailable = (currFragment.BytesRead + (prevFragment?.BytesRead ?? 0) - (prevFragment?.BytesProcessed ?? 0));
            var quotient = totalBytesAvailable / TangleDatumSize;            
            var remainder = totalBytesAvailable % TangleDatumSize;
            Interlocked.Add(ref _tangleMessageCount, quotient);

            if (remainder != 0)
            {
                currFragment.ProcessState = IoProducable<IoNetClient>.State.ConsumerFragmented;
                //_logger.Warn($"We got a fragmented message! kb = `{TotalBytesReceived / 1024}kb', Total = `{Interlocked.Read(ref _tangleMessageCount)}', batch =`{quotient}', Tail size = `{remainder}/{TangleDatumSize}' ({TangleDatumSize - remainder})");
            }
            else
            {
                currFragment.ProcessState = IoProducable<IoNetClient>.State.Consumed;
            }

            var buffer = (prevFragment == null ? currFragment .Buffer : prevFragment.Buffer.Skip(prevFragment.BytesProcessed).Concat(currFragment.Buffer)).ToArray();

            for (var i = 0; i < quotient; i++)
            {
                if ((i + 1) * (int) TangleDatumSize > buffer.Length)
                {
                    _logger.Debug($"Done calculating {quotient} crcs");
                    break;
                }

                var crc = new byte[parm_tangle_crc_size];
                for (var j = 0; j < 4; j++)
                {
                    try
                    {
                        if (buffer[(i+1) * (int) TangleDatumSize - parm_tangle_crc_size + j] != crc[j])
                        {
                            _logger.Warn($"CRC Failure at {j}");
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                }

                
            }

            currFragment.BytesProcessed = (int) (currFragment.BytesProcessed - remainder);

            return currFragment.ProcessState;
        }
    }
}
