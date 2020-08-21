﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace zero.core.patterns.bushes.contracts
{
    public interface IIoZero
    {
        IIoSource IoSource { get; }
        public object IoJobHeap { get; }
        string Description { get; }
        bool IsArbitrating { get; set; }
        bool Close();
        Task<bool> ProduceAsync(bool sleepOnConsumerLag = true);
        //Task<Task> ConsumeAsync(Func<IoLoad<IIoJob>, Task> inlineCallback = null, bool sleepOnProducerLag = true);
        Task SpawnProcessingAsync(bool spawnProducer = true);  
    }
}
