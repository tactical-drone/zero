using System;
using System.Threading.Tasks;

namespace zero.core.patterns.misc
{
    public class IoZeroSub
    {
        public object Action { get; private set; } = null;
        public object Target { get; private set; } = null;
        private object _state = null;
        public volatile bool Schedule = true;

        public IoZeroSub SetAction<T>(Func<IIoNanite, T, ValueTask> action, T state = default)
        {
            Action = action;
            Target = action.Target;
            _state = state;
            return this;
        }
        
        public Func<IIoNanite, T, ValueTask> GetAction<T>()
        {
            return (Func<IIoNanite, T, ValueTask>)Action;
        }
        
        public ValueTask ExecuteAsync<T>(IIoNanite nanite)
        {
            return GetAction<T>()(nanite, (T)_state);
        }
    }
}