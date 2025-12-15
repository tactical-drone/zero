using System;
using System.Threading.Tasks.Sources;
using zero.core.patterns.semaphore.core;

namespace zero.core.patterns.semaphore;

/// <summary>
///     ManualResetValueTaskSource
/// </summary>
/// <typeparam name="T">The result type, can be anything</typeparam>
public sealed class
    IoZeroResetValueTaskSource<T> : IIoManualResetValueTaskSourceCore<T> // IValueTaskSource<T>, IValueTaskSource
{
    private readonly IIoManualResetValueTaskSourceCore<T> _zeroCore;

    public IoZeroResetValueTaskSource(bool runContinuationsAsynchronously = false)
    {
        _zeroCore = new IoManualResetValueTaskSourceCore<T>
        {
            RunContinuationsAsynchronouslyAlways = runContinuationsAsynchronously,
            AutoReset = true
        };
    }

    public bool RunContinuationsAsynchronously
    {
        get => _zeroCore.RunContinuationsAsynchronouslyAlways;
        set => _zeroCore.RunContinuationsAsynchronouslyAlways = value;
    }

    public bool RunContinuationsAsynchronouslyAlways { get; set; }
    public bool AutoReset => _zeroCore.AutoReset;

    public void SetResult(T result)
    {
        _zeroCore.SetResult(result);
    }

    public void SetException(Exception exception)
    {
        _zeroCore.SetException(exception);
    }

    public void OnReset(Action<object> resetAction, object context)
    {
        throw new NotImplementedException();
    }

    public bool Blocking => _zeroCore.Blocking;

    public bool IsBlocking(bool reset)
    {
        throw new NotImplementedException();
    }

    public bool Primed => _zeroCore.Primed;
#if DEBUG
    public bool Burned => _zeroCore.Burned;
#endif
    public int SyncRoot
    {
        get => _zeroCore.SyncRoot;
        set => _zeroCore.SyncRoot = value;
    }

    public T GetResult(short token)
    {
        return _zeroCore.GetResult(token);
    }

    public ValueTaskSourceStatus GetStatus(short token)
    {
        return _zeroCore.GetStatus(token);
    }

    public void OnCompleted(Action<object> continuation, object state, short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        _zeroCore.OnCompleted(continuation, state, token, flags);
    }

    public void Reset()
    {
        _zeroCore.Reset();
    }

    public override string ToString()
    {
        return _zeroCore.ToString();
    }
}