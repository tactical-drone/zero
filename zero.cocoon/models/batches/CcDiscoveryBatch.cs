using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using zero.core.feat.models.bundle;
using zero.core.misc;

namespace zero.cocoon.models.batches;

public class CcDiscoveryBatch : IIoMessageBundle
{
    private int _count;

    public Dictionary<byte[], Tuple<byte[], List<CcBatchMessage>>> GroupBy;

    public CcDiscoveryBatch(int size, bool groupByEp = false)
    {
        Messages = new CcBatchMessage[size];
        GroupByEpEnabled = groupByEp;

        for (var i = 0; i < Messages.Length; i++)
            Messages[i] = new CcBatchMessage();

        if (GroupByEpEnabled)
            GroupBy = new Dictionary<byte[], Tuple<byte[], List<CcBatchMessage>>>(new IoByteArrayComparer());
    }

    public CcBatchMessage[] Messages { get; }

    public bool GroupByEpEnabled { get; }

    IIoBundleMessage IIoMessageBundle.this[int i]
    {
        get => Messages[i];
        set => Messages[i] = (CcBatchMessage)value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IIoBundleMessage Feed()
    {
        return Messages[Interlocked.Increment(ref _count) - 1];
    }

    public int Count => _count;
    public int Capacity => Messages.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset()
    {
        Interlocked.Exchange(ref _count, 0);
    }

    /// <summary>
    ///     Return this instance to the heap
    /// </summary>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReturnToHeap()
    {
        CcDiscoveries.Heap.Return(this);
    }
}