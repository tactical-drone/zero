using System;
using zero.core.conf;
using zero.core.patterns.bushes;
using zero.core.patterns.heap;

namespace zero.core.models
{
    /// <summary>
    /// Uses a network stream to fill the <see cref="Buffer"/> to <see cref="parm_max_recv_buf_size"/>
    /// </summary>
    public abstract class IoMessage<TSource> : IoConsumable<TSource>
    where TSource:IoConcurrentProcess
    {
        /// <summary>
        /// Initializes the buffer size to fill
        /// </summary>
        protected IoMessage()
        {
            Buffer = new byte[parm_max_recv_buf_size];

            //Set this instance to flush when settings change, new ones will be created with the correct settings
            SettingChangedEvent += (sender, pair) =>
            {
                if (pair.Key == nameof(parm_max_recv_buf_size))
                {
                    parm_max_recv_buf_size = (int) pair.Value;
                    Buffer = new byte[parm_max_recv_buf_size];
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
        public volatile int BytesProcessed;

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - BytesProcessed;

        /// <summary>
        /// Maximum message size in bytes that will be read at once
        /// </summary>        
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_max_recv_buf_size = 1650*10; // floor(msg_size /(msg_size - TCP_MTU))

        /// <summary>
        /// Prepares this item for use after being popped from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public override IOHeapItem Constructor()
        {
            BytesRead = 0;
            BytesProcessed = 0;
            return !Reconfigure ? base.Constructor() : null;
        }
    }
}
