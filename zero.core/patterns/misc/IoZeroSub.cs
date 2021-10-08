using System;
using System.Management;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using NLog;
using NLog.LayoutRenderers;

namespace zero.core.patterns.misc
{
    public class IoZeroSub
    {
        public object ZeroAction;
        public static readonly object ZeroSentinel = new();
        object State = ZeroSentinel;
        public object Target { get; private set; }
        public string From { get; }

        public volatile bool Schedule = true;

        public IoZeroSub(string from)
        {   
            From = from;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Func<IIoNanite, T, ValueTask<bool>> GenericCast<T>(T obj)
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

            if (closureState != null)
                State = closureState;
                
            Target = callback?.Target;
            return this;
        }

        public ValueTask<bool> ExecuteAsync(IIoNanite nanite)
        {
            try
            {
                return GenericCast((dynamic)State)(nanite, (dynamic)State);
            }
            catch (Exception e)
            {
                LogManager.GetCurrentClassLogger().Trace(e,$"{ZeroAction}, {Target}, {State}");
                return new ValueTask<bool>(false);
            }
        }

        public ValueTask ZeroAsync()
        {
            ZeroAction = null;
            State = null;
            Target = null;
            return ValueTask.CompletedTask;
        }
        
    }
}