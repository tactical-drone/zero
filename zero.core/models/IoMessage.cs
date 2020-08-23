using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;

namespace zero.core.models
{
    /// <inheritdoc />
    /// <summary>
    /// An abstract message carrier used by a network stream to fill the <see cref="F:zero.core.models.IoMessage`2.Buffer" />
    /// </summary>
    public abstract class IoMessage<TJob> : IoLoad<TJob>        
        where TJob : IIoJob 
        
    {
        /// <summary>
        /// Initializes the buffer size to fill
        /// </summary>
        protected IoMessage(string loadDescription, string jobDescription, IoSource<TJob> source) : base(loadDescription, jobDescription, source)
        {
            //Set this instance to flush when settings change, new ones will be created with the correct settings
            //SettingChangedEvent += (sender, pair) =>
            //{
            //    if (pair.Key == nameof(BufferSize))
            //    {
            //        BufferSize = (int) pair.Value;
            //        Buffer = new sbyte[BufferSize];
            //    }                    
            //};            
        }        

        /// <summary>
        /// A buffer to receive the message in
        /// </summary>
        public sbyte[] Buffer;


        /// <summary>
        /// A <see cref="byte"/> span of the input buffer
        /// </summary>
        public ReadOnlySpan<byte> BufferSpan => ByteBuffer.AsSpan();


        /// <summary>
        /// A byte array cast of the buffer
        /// </summary>
        public byte[] ByteBuffer => (byte[]) (Array) Buffer;


        /// <summary>
        /// Stream access to the input
        /// </summary>
        public Stream ByteStream => new MemoryStream(ByteBuffer, BufferOffset, BytesLeftToProcess);
        
        /// <summary>
        /// The number of bytes read into the buffer
        /// </summary>
        public volatile int BytesRead;

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - (BufferOffset - DatumProvisionLengthMax);

        /// <summary>
        /// The number of bytes processed from the buffer
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
        public volatile int DatumSize;

        /// <summary>
        /// The length of the buffer offset to allow previous fragments to be concatenated to the current buffer
        /// </summary>
        public volatile int DatumProvisionLengthMax;

        /// <summary>
        /// The length of the buffer offset to allow previous fragments to be concatenated to the current buffer
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
        public override IIoHeapItem Constructor()
        {
            BytesRead = 0;
            DatumProvisionLength = DatumProvisionLengthMax;
            BufferOffset = DatumProvisionLengthMax;
            
            //return !Reconfigure ? base.Constructor() : null; //TODO what was this about?
            return base.Constructor();
        }

        /// <summary>
        /// Does dup checking on this transaction
        /// </summary>        
        /// <returns></returns>
        public async Task<bool> WasProcessedRecentlyAsync(string key)
        {            
            return await Source.RecentlyProcessed.KeyExistsAsync(key).ConfigureAwait(false);            
        }
    }
}
