using System;

namespace zero.core.patterns.misc
{
    /// <summary>
    /// Creates a disposable that calls an anonymous function on dispose
    /// </summary>
    public static class IoAnonymousDisposable
    {
        /// <summary>
        /// Creates a anonymous disposable.
        /// </summary>
        /// <param name="action">The action to be performed</param>
        /// <returns>A handle to the disposable</returns>
        public static IDisposable Create(Action action)
        {
            return new AnonymousDisposable(action);
        }

        /// <summary>
        /// A empty disposable
        /// </summary>
        /// <seealso cref="System.IDisposable" />
        private struct AnonymousDisposable : IDisposable
        {
            /// <summary>
            /// The dispose callback.
            /// </summary>
            private readonly Action _dispose;

            /// <summary>
            /// Initializes a new instance of the <see cref="AnonymousDisposable"/> struct.
            /// </summary>
            /// <param name="dispose">The callback called on dispose.</param>
            public AnonymousDisposable(Action dispose)
            {
                _dispose = dispose;
            }

            /// <summary>
            /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            public void Dispose()
            {
                _dispose?.Invoke();
            }
        }
    }
}
