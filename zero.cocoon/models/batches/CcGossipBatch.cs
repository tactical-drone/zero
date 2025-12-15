using System.Threading;
using zero.core.feat.models.bundle;

namespace zero.cocoon.models.batches;

public class CcGossipBatch : IIoMessageBundle
{
    private readonly CcBatchMessage[] _messages;

    private int _count;

    public CcGossipBatch(int size)
    {
        Capacity = size;
        _messages = new CcBatchMessage[Capacity];
        for (var i = 0; i < _messages.Length; i++)
            _messages[i] = new CcBatchMessage();
    }

    IIoBundleMessage IIoMessageBundle.this[int i]
    {
        get => _messages[i];
        set => Interlocked.Exchange(ref _messages[i], (CcBatchMessage)value);
    }

    public IIoBundleMessage Feed()
    {
        return _messages[Interlocked.Increment(ref _count) - 1];
    }

    public int Count => _count;
    public int Capacity { get; }

    public void Reset()
    {
        Interlocked.Exchange(ref _count, 0);
    }
}