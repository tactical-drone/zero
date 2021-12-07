using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using NLog;
using zero.core.patterns.heap;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch: IDisposable
    {
        public CcDiscoveryBatch(IoHeap<CcDiscoveryBatch, CcDiscoveries> heapRef, int size)
        {
            _heapRef = heapRef;
            _messages = ArrayPool<CcDiscoveryMessage>.Shared.Rent(size);
            for (var i = 0; i < _messages.Length; i++)
            {
                _messages[i] = new CcDiscoveryMessage();
            }
        }

        readonly IoHeap<CcDiscoveryBatch, CcDiscoveries> _heapRef;
        private readonly CcDiscoveryMessage[] _messages;
        private readonly Dictionary<string, ReadOnlyMemory<CcDiscoveryMessage>> _messagesDictionary = new();
        private volatile int _disposed;

        public CcDiscoveryMessage this[int i] => _messages[i];

        public CcDiscoveryMessage[] Messages => _messages;

        /// <summary>
        /// Return this instance to the heap
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReturnToHeap()
        {
            if(_heapRef.Count / (double)_heapRef.MaxSize > 0.8)
                LogManager.GetCurrentClassLogger().Warn($"{nameof(CcDiscoveryBatch)}: Heap is running lean, {_heapRef} ");

            foreach (var t in _messages)
            {
                t.Message = null;
                t.EmbeddedMsg = null;
            }

            _heapRef.Return(this);
        }

        public IoHeap<CcDiscoveryBatch, CcDiscoveries> HeapRef => _heapRef;

        public int Capacity => _messages.Length;
        public volatile int Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;

            if (disposing)
            {
                ReturnToHeap();

                try
                {
                    ArrayPool<CcDiscoveryMessage>.Shared.Return(_messages, true);
                }
                catch
                {
                    // ignored
                }
            }

            //_messages = null;
            //_heapRef = null;
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
