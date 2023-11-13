using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace zero.core.runtime.threadpool
{
    public class IoThreadPoolHooks<TState>
    {
        private IoThreadPoolHooks(Func<Action<TState>, TState, bool, bool> hook)
        {
            _unsafeQueueUserWorkItem = hook;
        }

        private readonly Func<Action<TState>, TState, bool, bool> _unsafeQueueUserWorkItem;
        public static IoThreadPoolHooks<TState> Default { get; private set; }

        public static void Init(Func<Action<TState>, TState, bool, bool> unsafeQueueUserWorkItem) 
        {
            Default = new IoThreadPoolHooks<TState>(unsafeQueueUserWorkItem);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool UnsafeQueueUserWorkItem(Action<TState> callBack, TState state, bool preferLocal = true)
        {
            if (Default != null)
            {
                return Default._unsafeQueueUserWorkItem(callBack, state, preferLocal);
            }
            else
            {
                return ThreadPool.UnsafeQueueUserWorkItem(WaitCallback, (callBack, state));
                static void WaitCallback(object context)
                {
                    var (continuation, state) = (ValueTuple<Action<object>, object>)context;
                    continuation(state);
                }
            }
        }
    }
}
