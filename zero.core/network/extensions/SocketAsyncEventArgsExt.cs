using System;
using System.Net.Sockets;

namespace zero.core.network.extensions
{
    public class SocketAsyncEventArgsExt : SocketAsyncEventArgs, IDisposable
    {
        public bool Disposed => _disposed;
        private bool _disposed = false;
        private void ReleaseUnmanagedResources()
        {
            // TODO release unmanaged resources here
            base.Dispose();
        }

        protected virtual void Dispose(bool disposing)
        {
            _disposed = true;
            ReleaseUnmanagedResources();
            if (disposing)
            {
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SocketAsyncEventArgsExt()
        {
            Dispose(false);
        }
    }
}
