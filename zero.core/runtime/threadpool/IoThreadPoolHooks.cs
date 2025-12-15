using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.runtime.threadpool;

public class IoThreadPoolHooks<TState>
{
    private readonly Func<Action<TState>, TState, bool, bool> _unsafeQueueUserWorkItem;

    private IoThreadPoolHooks(Func<Action<TState>, TState, bool, bool> hook)
    {
        _unsafeQueueUserWorkItem = hook;
    }

    public static IoThreadPoolHooks<TState> Default { get; private set; }

    public static void Init(Func<Action<TState>, TState, bool, bool> unsafeQueueUserWorkItem)
    {
        Default = new IoThreadPoolHooks<TState>(unsafeQueueUserWorkItem);
    }

    private static void WaitCallback(object context)
    {
        var (continuation, state) = (ValueTuple<Action<object>, object>)context;
        continuation(state);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool UnsafeQueueUserWorkItem(Action<TState> callBack, TState state, bool preferLocal = true)
    {
        return Default != null
            ? Default._unsafeQueueUserWorkItem(callBack, state, preferLocal)
            : ThreadPool.UnsafeQueueUserWorkItem(WaitCallback, (callBack, state));
    }
}