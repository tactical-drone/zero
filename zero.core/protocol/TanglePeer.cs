using System.Linq;
using System.Text;
using System.Threading;
using NLog;
using zero.core.core;
using zero.core.models;
using zero.core.network.ip;
using zero.core.patterns.bushes;

namespace zero.core.protocol
{
    /// <summary>
    /// The iota protocol
    /// </summary>
    public class TanglePeer :IoNeighbor
    {                                
        /// <summary>
        /// Constructs a IOTA tangle neighbor handler
        /// </summary>
        /// <param name="ioNetClient">The network client used to communicate with this neighbor</param>
        public TanglePeer(IoNetClient ioNetClient): 
            base(ioNetClient,()=>new IoP2Message(ioNetClient, DatumLength) { JobDescription = $"rx", WorkDescription = $"{ioNetClient.Address}" })
        {
            _logger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// The logger
        /// </summary>
        private readonly Logger _logger;

        /// <summary>
        /// The number of protocol messages that have been received
        /// </summary>
        private long _tangleMessageCount = 0;

        /// <summary>
        /// The length of tangle protocol messages
        /// </summary>
        private static readonly int MessageLength = 1650;

        /// <summary>
        /// The size of tangle protocol messages crc
        /// </summary>
        private static readonly int MessageCrcLength = 16;

        /// <summary>
        /// The protocol message length
        /// </summary>
        private static readonly int DatumLength = MessageLength + MessageCrcLength;

        /// <summary>
        /// Does work on the messages received
        /// </summary>
        /// <param name="currJob">The current job fragment to be procesed</param>
        /// <param name="currJobPreviousFragment">Include a previous job fragment if that job had a fragmented datum</param>
        /// <returns>The state of work done</returns>
        protected override IoProducable<IoNetClient>.State Consume(IoMessage<IoNetClient> currJob, IoMessage<IoNetClient> currJobPreviousFragment = null)
        {
            IoProducable<IoNetClient>.State produceState;
            if ((produceState = base.Consume(currJob)) >= IoProducable<IoNetClient>.State.Error)
                return produceState;

            //TODO Find a more elegant way
            if (TotalBytesReceived == 10)
            {
                _logger.Trace($"Got receiver port as: `{Encoding.ASCII.GetString(currJob.Buffer).Substring(0,10)}'");
                return currJob.ProcessState = IoProducable<IoNetClient>.State.Consumed;
            }

            //Calculate how many complete protocol messages we have received   
            var totalBytesAvailable = (currJob.BytesRead + (currJobPreviousFragment?.BytesRead ?? 0) - (currJobPreviousFragment?.BytesProcessed ?? 0));
            var quotient = totalBytesAvailable / DatumLength;            
            var remainder = totalBytesAvailable % DatumLength;
            Interlocked.Add(ref _tangleMessageCount, quotient);
            
            //TODO linq might not be most optimum for this step
            //Recombine datum fragments
            //var buffer = (currJobPreviousFragment == null ? currJob .Buffer : currJobPreviousFragment.Buffer.Skip(currJobPreviousFragment.BytesProcessed).Concat(currJob.Buffer)).ToArray();

            //For each datum received
            for (var i = 0; i < quotient; i++)
            {
                //if ((i + 1) * DatumLength > buffer.Length)
                {
                    _logger.Debug($"Processed `{quotient}' datums, remainder = `{remainder}', totalBytesAvailable = `{totalBytesAvailable}', currJob.BytesRead = `{currJob.BytesRead}', prevJob.BytesLeft =`{currJobPreviousFragment?.BytesRead - currJobPreviousFragment?.BytesProcessed}'");
                    break;
                }

                //TODO I cannot find a C# crc32 that works with the Java one
                //var crc = new byte[parm_tangle_crc_size];
                //for (var j = 0; j < 4; j++)
                //{
                //    try
                //    {
                //        if (buffer[(i+1) * (int) _datumLength - parm_tangle_crc_size + j] != crc[j])
                //        {
                //            _logger.Warn($"CRC Failure at {j}");
                //            break;
                //        }
                //    }
                //    catch (Exception e)
                //    {
                //        Console.WriteLine(e);
                //        throw;
                //    }
                //}                
            }

            currJob.BytesProcessed = currJob.BytesRead - remainder;

            currJob.ProcessState = remainder != 0 ? IoProducable<IoNetClient>.State.ConsumerFragmented : IoProducable<IoNetClient>.State.Consumed;

            return currJob.ProcessState;
        }
    }
}
