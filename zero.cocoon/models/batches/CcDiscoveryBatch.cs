﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using zero.core.feat.models.bundle;
using zero.core.misc;

namespace zero.cocoon.models.batches
{
    public class CcDiscoveryBatch: IIoMessageBundle
    {
        public CcDiscoveryBatch(int size, bool groupByEp = false)
        {
            _messages = new CcBatchMessage[size];
            _groupByEpEnabled = groupByEp;

            for (var i = 0; i < _messages.Length; i++)
                _messages[i] = new CcBatchMessage();

            if (_groupByEpEnabled)
                GroupBy = new Dictionary<byte[], Tuple<byte[], List<CcBatchMessage>>>(new IoByteArrayComparer());
        }

        private readonly CcBatchMessage[] _messages;

        public CcBatchMessage[] Messages => _messages;

        public Dictionary<byte[], Tuple<byte[], List<CcBatchMessage>>> GroupBy;

        /// <summary>
        /// Return this instance to the heap
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReturnToHeap()
        {
            CcDiscoveries.Heap.Return(this);
        }

        private int _count;
        IIoBundleMessage IIoMessageBundle.this[int i]
        {
            get => _messages[i];
            set => _messages[i] = (CcBatchMessage)value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IIoBundleMessage Feed() => _messages[Interlocked.Increment(ref _count) - 1];
        public int Count => _count;
        public int Capacity => _messages.Length;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            Interlocked.Exchange(ref _count, 0);
        }

        private readonly bool _groupByEpEnabled;
        public bool GroupByEpEnabled => _groupByEpEnabled;
    }
}
