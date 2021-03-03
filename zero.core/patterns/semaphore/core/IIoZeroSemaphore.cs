﻿using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace zero.core.patterns.semaphore.core
{
    public interface IIoZeroSemaphore: IValueTaskSource<bool>
    {
        void ZeroRef(ref IIoZeroSemaphore @ref, CancellationToken asyncToken);
        int Release(int releaseCount = 1);
        ValueTask<bool> WaitAsync();
        void Zero();
        int CurrentCount { get; }
    }
}