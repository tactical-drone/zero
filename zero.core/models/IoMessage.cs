using System;
using zero.core.conf;
using zero.core.patterns.bushes;
using zero.core.patterns.heap;

namespace zero.core.models
{
    /// <summary>
    /// Uses a network stream to fill the <see cref="Buffer"/> to <see cref="MaxRecvBufSize"/>
    /// </summary>
    public abstract class IoMessage<TSource> : IoConsumable<TSource>
    where TSource:IoConcurrentProcess
    {
        /// <summary>
        /// Initializes the buffer size to fill
        /// </summary>
        protected IoMessage(int datumLength)
        {            
            DatumLength = datumLength;
            MaxRecvBufSize = DatumLength * (parm_max_datum_buffer_size + 1) - 1;
            Buffer = new byte[MaxRecvBufSize];

            //Set this instance to flush when settings change, new ones will be created with the correct settings
            SettingChangedEvent += (sender, pair) =>
            {
                if (pair.Key == nameof(MaxRecvBufSize))
                {
                    MaxRecvBufSize = (int) pair.Value;
                    Buffer = new byte[MaxRecvBufSize];
                }                    
            };
        }

        /// <summary>
        /// A buffer to receive the message in
        /// </summary>
        public byte[] Buffer;
        
        /// <summary>
        /// The number of bytes read into the buffer
        /// </summary>
        public volatile int BytesRead;

        /// <summary>
        /// The number of bytes proccessed from the buffer
        /// </summary>
        public volatile int BufferOffset;

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - BufferOffset;

        /// <summary>
        /// Total number of datums contained inside the buffer
        /// </summary>
        public int DatumCount { get; set; }

        /// <summary>
        /// The number of bytes remaining of a fragmented datum
        /// </summary>
        public int DatumFragmentLength { get; set; }

        /// <summary>
        /// The expected datum length
        /// </summary>
        public int DatumLength;

        /// <summary>
        /// Message buffer receive length
        /// </summary>        
        public int MaxRecvBufSize;
        
        /// <summary>
        /// Maximul number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_datum_buffer_size = 10;

        /// <summary>
        /// Prepares this item for use after being popped from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public override IOHeapItem Constructor()
        {
            BytesRead = DatumLength - 1;
            BufferOffset = BytesRead;
            return !Reconfigure ? base.Constructor() : null;
        }
    }
}
