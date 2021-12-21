using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using NLog;
using zero.core.misc;
using zero.core.patterns.heap;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch: IDisposable
    {
        public CcDiscoveryBatch(IoHeap<CcDiscoveryBatch, CcDiscoveries> heapRef, int size, bool groupByEp = false)
        {
            _heapRef = heapRef;
            _messages = ArrayPool<CcDiscoveryMessage>.Shared.Rent(size);
            _groupByEpEnabled = groupByEp;

            for (var i = 0; i < _messages.Length; i++)
            {
                _messages[i] = new CcDiscoveryMessage();
            }

            if (_groupByEpEnabled)
                GroupBy = new Dictionary<byte[], Tuple<byte[], List<CcDiscoveryMessage>>>(new IoByteArrayComparer());
        }

        IoHeap<CcDiscoveryBatch, CcDiscoveries> _heapRef;
        private CcDiscoveryMessage[] _messages;
        private Dictionary<string, ReadOnlyMemory<CcDiscoveryMessage>> _messagesDictionary = new();
        private volatile int _disposed;
        private readonly bool _groupByEpEnabled;

        public CcDiscoveryMessage this[int i] => _messages[i];

        public CcDiscoveryMessage[] Messages => _messages;

        public Dictionary<byte[], Tuple<byte[], List<CcDiscoveryMessage>>> GroupBy;

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
                t.Chroniton = null;
                t.EmbeddedMsg = null;
            }

            _heapRef.Return(this);
        }

        public IoHeap<CcDiscoveryBatch, CcDiscoveries> HeapRef => _heapRef;

        public int Capacity => _messages.Length;
        public volatile int Count;
        public bool GroupByEpEnabled => _groupByEpEnabled;

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

            _messages = null;
            _heapRef = null;
            _messagesDictionary.Clear();
            _messagesDictionary = null;
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
