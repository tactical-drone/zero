﻿using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace zero.core.patterns.misc
{
    public class IoZeroSub
    {
        public object ZeroAction;
        public static readonly object ZeroSentinel = new();
        object _state = ZeroSentinel;
        private int _executed;
        public object Target { get; private set; }
        public string From { get; }
        public bool Executed => _executed > 0;

        public IoZeroSub(string from)
        {   
            From = from;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0060 // Remove unused parameter
        public Func<IIoNanite, T, ValueTask<bool>> ZeroFunc<T>(T obj)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            try
            {
                return (Func<IIoNanite, T, ValueTask<bool>>) ZeroAction;
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Error(e);
                throw;
            }
        }

        public IoZeroSub SetAction<T>(Func<IIoNanite, T, ValueTask<bool>> callback, T closureState = default)
        {
            ZeroAction = callback;            
            _state = closureState;                
            Target = callback?.Target;
            return this;
        }

        public ValueTask<bool> ExecuteAsync(IIoNanite @from)
        {
            try
            {
                if (_executed > 0 ||  Interlocked.CompareExchange(ref _executed, 1, 0) != 0)
                    return new ValueTask<bool>(true);

                return ZeroFunc((dynamic)_state)(@from, (dynamic)_state);
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Fatal(e, $"{ZeroAction}, {Target}, {_state}");
                return new ValueTask<bool>(false);
            }
            finally
            {
                ZeroAction = default;
                _state = default;
                Target = default;
            }
        }
    }
}