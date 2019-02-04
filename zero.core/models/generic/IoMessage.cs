using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.Extensions.FileSystemGlobbing.Internal.PatternContexts;
using zero.core.data.providers.redis;
using zero.core.patterns.bushes;
using zero.core.patterns.bushes.contracts;
using zero.core.patterns.heap;
using zero.interop.entangled.common.model.interop;

namespace zero.core.models.generic
{
    /// <inheritdoc />
    /// <summary>
    /// An abstract message carrier used by a network stream to fill the <see cref="F:zero.core.models.IoMessage`2.Buffer" />
    /// </summary>
    public abstract class IoMessage<TJob> : IoConsumable<TJob>        
        where TJob : IIoWorker 
        
    {
        /// <summary>
        /// Initializes the buffer size to fill
        /// </summary>
        protected IoMessage()            
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
        /// The number of bytes read into the buffer
        /// </summary>
        public volatile int BytesRead;

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
        public async Task<bool> DupCheck(string key)
        {
            if(ProducerHandle.DupChecker.IsConnected)
                return await ProducerHandle.DupChecker.IsDuplicate(key);
            return false;
        }
    }
}
