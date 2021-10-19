﻿using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.heap;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch: IDisposable
    {
        public CcDiscoveryBatch(IoHeap<CcDiscoveryBatch, CcDiscoveries> heapRef, int size)
        {
            _heapRef = heapRef;
            _messages = ArrayPool<CcDiscoveryMessage>.Shared.Rent(size);
            for (int i = 0; i < _messages.Length; i++)
            {
                _messages[i] = new CcDiscoveryMessage();
            }
        }


        IoHeap<CcDiscoveryBatch, CcDiscoveries> _heapRef;
        CcDiscoveryMessage[] _messages;
        public volatile string RemoteEndPoint;
        private int _disposed;

        public CcDiscoveryMessage this[int i]
        {
            get => _messages[i];
            set => _messages[i] = value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReturnToHeapAsync()
        {
            return _heapRef.ReturnAsync(this);
        }

        public IoHeap<CcDiscoveryBatch, CcDiscoveries> HeapRef => _heapRef;

        public int Count => _messages.Length;

        public int Filled;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;
            
            if (disposing)
            {
                try
                {
                    ArrayPool<CcDiscoveryMessage>.Shared.Return(_messages);
                }
                catch
                {
                    // ignored
                }
            }

            _messages = null;
            _heapRef = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~CcDiscoveryBatch()
        {
            Dispose(false);
        }
    }
}