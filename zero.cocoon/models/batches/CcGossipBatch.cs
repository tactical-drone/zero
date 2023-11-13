using System.Threading;
using zero.core.feat.models.bundle;

namespace zero.cocoon.models.batches
{ 
    public class CcGossipBatch :IIoMessageBundle
    {
        public CcGossipBatch(int size)
        {
            _capacity = size;
            _messages = new CcBatchMessage[_capacity];
            for (var i = 0; i < _messages.Length; i++)
                _messages[i] = new CcBatchMessage();
        }
        private readonly CcBatchMessage[] _messages;
        
        IIoBundleMessage IIoMessageBundle.this[int i]
        {
            get => _messages[i];
            set => Interlocked.Exchange(ref _messages[i], (CcBatchMessage)value);
        }

        private int _count;
        private readonly int _capacity;
        public IIoBundleMessage Feed() => _messages[Interlocked.Increment(ref _count) -1];
        public int Count => _count;
        public int Capacity => _capacity;
        public void Reset()
        {
            Interlocked.Exchange(ref _count, 0);
        }
    }
}
