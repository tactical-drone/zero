using System;

namespace zero.core.patterns.misc
{
    public static class IoAnonymousDiposable
    {
        public static IDisposable Create(Action action)
        {
            return new AnonymousDisposable(action);
        }
        private struct AnonymousDisposable : IDisposable
        {
            private readonly Action _dispose;
            public AnonymousDisposable(Action dispose)
            {
                _dispose = dispose;
            }
            public void Dispose()
            {
                _dispose?.Invoke();
            }
        }
    }
}
