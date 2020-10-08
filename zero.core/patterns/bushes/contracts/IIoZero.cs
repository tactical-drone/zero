using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Protocol;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoZero: IIoNanoprobe
    {
        IIoSource IoSource { get; }
        //public object IoJobHeap { get; }
        bool IsArbitrating { get; }

        /// <summary>
        /// Number of concurrent producers
        /// </summary>
        int ProducerCount { get; }

        /// <summary>
        /// Number of concurrent consumers
        /// </summary>
        int ConsumerCount { get; }

        Task AssimilateAsync();
        ValueTask<bool> ConsumeAsync();
    }
}
