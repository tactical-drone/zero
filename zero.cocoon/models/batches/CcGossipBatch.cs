using System.Threading;
using zero.core.feat.models.bundle;

namespace zero.cocoon.models.batches
{ 
    public class CcGossipBatch :IIoMessageBundle
    {
        public CcGossipBatch(int size)
        {
            _messages = new CcBatchMessage[size];
            for (var i = 0; i < _messages.Length; i++)
                _messages[i] = new CcBatchMessage();
        }
        private CcBatchMessage[] _messages;
        public void Dispose()
        {
            _messages = null;
        }

        IIoBundleMessage IIoMessageBundle.this[int i]
        {
            get => _messages[i];
            set => Interlocked.Exchange(ref _messages[i], (CcBatchMessage)value);
        }

        private int _count;
        public IIoBundleMessage Feed => _messages[Interlocked.Increment(ref _count) -1];
        public int Count { get; }
        public int Capacity { get; }
        public void Reset()
        {
            Interlocked.Exchange(ref _count, 0);
        }
    }
}
