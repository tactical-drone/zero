using System;
using System.Management;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using NLog.LayoutRenderers;

namespace zero.core.patterns.misc
{
    public class IoZeroSub
    {
        public object ZeroAction;
        public static readonly object ZeroSentinel = new();
        public object State = ZeroSentinel;
        public object Target { get; private set; }
        public string From { get; }

        public volatile bool Schedule = true;

        public IoZeroSub(string from)
        {   
            From = from;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Func<IIoNanite, T, ValueTask> GenericCast<T>(T obj)
        {
            try
            {
                return (Func<IIoNanite, T, ValueTask>) ZeroAction;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public IoZeroSub SetAction<T>(Func<IIoNanite, T, ValueTask> callback, T closureState = default)
        {
            ZeroAction = callback;

            if (closureState != null)
                State = closureState;
                
            Target = callback?.Target;
            return this;
        }

        public ValueTask ExecuteAsync(IIoNanite nanite)
        {
            try
            {
                return GenericCast((dynamic)State)(nanite, (dynamic)State);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}