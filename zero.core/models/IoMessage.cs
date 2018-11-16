﻿using System;
using zero.core.conf;
using zero.core.patterns.bushes;
using zero.core.patterns.heap;
using zero.core.protocol;

namespace zero.core.models
{
    /// <summary>
    /// An abstract message carrier used by a network stream to fill the <see cref="Buffer"/>
    /// </summary>
    public abstract class IoMessage<TSource> : IoConsumable<TSource>
    where TSource:IoJobSource
    {
        /// <summary>
        /// Initializes the buffer size to fill
        /// </summary>
        protected IoMessage()
        {
            //Set this instance to flush when settings change, new ones will be created with the correct settings
            SettingChangedEvent += (sender, pair) =>
            {
                if (pair.Key == nameof(BufferSize))
                {
                    BufferSize = (int) pair.Value;
                    Buffer = new sbyte[BufferSize];
                }                    
            };
        }

        /// <summary>
        /// A buffer to receive the message in
        /// </summary>
        public sbyte[] Buffer;
        
        /// <summary>
        /// The number of bytes read into the buffer
        /// </summary>
        public volatile int BytesRead;

        /// <summary>
        /// The number of bytes proccessed from the buffer
        /// </summary>
        public volatile int BufferOffset;

        /// <summary>
        /// Total number of datums contained inside the buffer
        /// </summary>
        public volatile int DatumCount;

        /// <summary>
        /// The number of bytes remaining of a fragmented datum
        /// </summary>
        public volatile int DatumFragmentLength;

        /// <summary>
        /// The expected datum length
        /// </summary>
        public volatile int DatumLength;

        /// <summary>
        /// The length of the buffer offset to allow previous fragments to be concatinated to the current buffer
        /// </summary>
        public volatile int DatumProvisionLength;

        /// <summary>
        /// Message receive buffer size
        /// </summary>        
        public volatile int BufferSize;
   
        /// <summary>
        /// Prepares this item for use after being popped from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public override IOHeapItem Constructor()
        {
            BytesRead = 0;
            DatumProvisionLength = DatumLength - 1;
            BufferOffset = DatumProvisionLength;
            return !Reconfigure ? base.Constructor() : null;
        }
    }
}
