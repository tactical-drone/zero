using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using zero.core.misc;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch: IDisposable
    {
        public CcDiscoveryBatch(int size, bool groupByEp = false)
        {
            _messages = new CcDiscoveryMessage[size];
            _groupByEpEnabled = groupByEp;

            for (var i = 0; i < _messages.Length; i++)
                _messages[i] = new CcDiscoveryMessage();

            if (_groupByEpEnabled)
                GroupBy = new Dictionary<byte[], Tuple<byte[], List<CcDiscoveryMessage>>>(new IoByteArrayComparer());
        }

        private CcDiscoveryMessage[] _messages;
        private int _disposed;

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
            CcDiscoveries.Heap.Return(this, _disposed > 0);
        }

        public int Capacity => _messages.Length;
        public int Count;
        
        private readonly bool _groupByEpEnabled;
        public bool GroupByEpEnabled => _groupByEpEnabled;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                return;

            _messages = null;
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
