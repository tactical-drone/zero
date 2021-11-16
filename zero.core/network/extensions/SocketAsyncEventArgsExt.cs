using System;
using System.Net.Sockets;
using System.Threading;

namespace zero.core.network.extensions
{
    public sealed class SocketAsyncEventArgsExt : SocketAsyncEventArgs, IDisposable
    {
        public SocketAsyncEventArgsExt()
        {
            
        }
        public bool Disposed => _disposed == 1;
        private volatile int _disposed;
        private void ReleaseUnmanagedResources()
        {
            // TODO release unmanaged resources here
            base.Dispose();
        }

        private void Dispose(bool disposing)
        {
            if( Interlocked.CompareExchange(ref _disposed, 1, 0) == 0 ) 
                ReleaseUnmanagedResources();
        }

        void IDisposable.Dispose()
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
