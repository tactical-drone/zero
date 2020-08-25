using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Protocol;
using zero.core.patterns.misc;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoZero: IIoZeroable
    {
        IIoSource IoSource { get; }
        public object IoJobHeap { get; }
        bool IsArbitrating { get; set; }
        Task<bool> ProduceAsync(bool sleepOnConsumerLag = true);
        //Task<Task> ConsumeAsync(Func<IoLoad<IIoJob>, Task> inlineCallback = null, bool sleepOnProducerLag = true);
        Task SpawnProcessingAsync(bool spawnProducer = true);  
    }
}
