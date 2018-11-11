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

            //TODO Find a more elegant way for this terrible hack
            //Disgard the neighbor port data
            if (TotalMessagesCount == 0)
            {
                _logger.Trace($"Got receiver port as: `{Encoding.ASCII.GetString(currJob.Buffer).Substring(0,10)}'");
                currJob.BufferOffset += 10;
                if( currJob.BytesLeftToProcess == 0 )
                    return currJob.ProcessState = IoProducable<IoNetClient>.State.Consumed;
            }

            //Process messages received
            _logger.Debug($"Processed `{currJob.DatumCount}' datums, remainder = `{currJob.DatumFragmentLength}', totalBytesAvailable = `{currJob.BytesRead}', currJob.BytesRead = `{currJob.BytesRead}', prevJob.BytesLeftToProcess =`{currJobPreviousFragment?.BytesRead - currJobPreviousFragment?.BufferOffset}'");
            

            currJob.BufferOffset += currJob.BytesLeftToProcess - currJob.DatumFragmentLength;            
            currJob.ProcessState = currJob.DatumFragmentLength != 0 ? IoProducable<IoNetClient>.State.ConsumerFragmented : IoProducable<IoNetClient>.State.Consumed;

            return currJob.ProcessState;
        }
    }
}
