using System.Threading.Tasks;
using NLog;

namespace zero.core.patterns.bushings.contracts
{
    public class IoZeroSource: IoSource<IoZeroProduct>
    {
        public IoZeroSource(string description, bool proxy, int prefetchSize = 1, int concurrencyLevel = 1, bool zeroAsyncMode = false, bool disableZero = false) : base(description, proxy, prefetchSize, concurrencyLevel, zeroAsyncMode)
        {
            Key = $"{nameof(IoZeroSource)}: {Serial}";
            _logger = LogManager.GetCurrentClassLogger();
        }

        private readonly ILogger _logger;

        public override string Key { get; }


        public override ValueTask ZeroManagedAsync()
        {
            return base.ZeroManagedAsync();
        }

        public bool Produce()
        {
            return IsOperational();
        }
    }
}
