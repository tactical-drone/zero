using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
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
    public abstract class IoMessage<TJob> : IoSink<TJob>        
        where TJob : IIoJob 
        
    {
        /// <summary>
        /// Initializes the buffer size to fill
        /// </summary>
        protected IoMessage(string sinkDesc, string jobDesc, IoSource<TJob> source) : base(sinkDesc, jobDesc, source)
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
        /// buffer owner
        /// </summary>
        public IMemoryOwner<sbyte> MemoryOwner;

        /// <summary>
        /// A buffer to receive the message in
        /// </summary>
        public sbyte[] Buffer;

        /// <summary>
        /// A clone
        /// </summary>
        public byte[] BufferClone;

        /// <summary>
        /// A memory clone
        /// </summary>
        public Memory<byte> BufferCloneMemory;

        /// <summary>
        /// A <see cref="byte"/> span of the input buffer
        /// </summary>
        public Span<byte> BufferSpan => ByteBuffer.AsSpan();

        /// <summary>
        /// A byte array cast of the buffer
        /// </summary>
        public byte[] ByteBuffer => (byte[]) (Array) Buffer;

        /// <summary>
        /// Array segment form
        /// </summary>
        public ArraySegment<byte> ByteSegment { get; protected set; }

        /// <summary>
        /// A memory buffer
        /// </summary>
        public Memory<byte> MemoryBuffer { get; protected set; }

        /// <summary>
        /// Pins the memory
        /// </summary>
        public MemoryHandle MemoryBufferPin { get; protected set; }

        /// <summary>
        /// Stream access to the input
        /// </summary>
        public Stream ByteStream;

        /// <summary>
        /// Coded Stream
        /// </summary>
        public CodedInputStream CodedStream;
        
        /// <summary>
        /// Read only sequence wrapped for protobuf API
        /// </summary>
        public ReadOnlySequence<byte> ReadOnlySequence { get; protected set; }

        /// <summary>
        /// The number of bytes read into the buffer
        /// </summary>
        public volatile int BytesRead;

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - (BufferOffset - DatumProvisionLengthMax);

        /// <summary>
        /// The current offset
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

        ///// <summary>
        ///// The length of the buffer offset to allow previous fragments to be concatenated to the current buffer
        ///// </summary>
        //public volatile int DatumProvisionLength;

        /// <summary>
        /// Message receive buffer size
        /// </summary>        
        public volatile int BufferSize;
   
        /// <summary>
        /// Prepares this item for use after being popped from the heap
        /// </summary>
        /// <returns>This instance</returns>
        public override ValueTask<IIoHeapItem> ConstructorAsync()
        {
            BytesRead = 0;
            //DatumProvisionLength = DatumProvisionLengthMax;
            BufferOffset = DatumProvisionLengthMax;
            
            //return !Reconfigure ? base.Constructor() : null; //TODO what was this about?
            return base.ConstructorAsync();
        }

        /// <summary>
        /// Does dup checking on this transaction
        /// </summary>        
        /// <returns></returns>
        public async Task<bool> WasProcessedRecentlyAsync(string key)
        {            
            return await Source.RecentlyProcessed.KeyExistsAsync(key).ConfigureAwait(false);
        }

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            Buffer = null;
            ByteSegment = null;
#endif
        }

        /// <summary>
        /// Handle fragments
        /// </summary>
        public override void SyncPrevJob()
        {
            if (!IoZero.SyncRecoveryModeEnabled || !(PreviousJob?.StillHasUnprocessedFragments ?? false)) return;

            var p = (IoMessage<TJob>)PreviousJob;
            try
            {
                var bytesLeft = Math.Min(p.DatumFragmentLength, DatumProvisionLengthMax);
                Interlocked.Add(ref BufferOffset, -bytesLeft);
                Interlocked.Add(ref BytesRead, bytesLeft);

                p.MemoryBuffer.Slice(p.BufferOffset + p.BytesRead - p.BytesLeftToProcess).CopyTo(MemoryBuffer[BufferOffset..]);
                
                p.State = IoJobMeta.JobState.Consumed;
                p.State = IoJobMeta.JobState.Accept;

                _logger.Error($"{Description}: >>> {bytesLeft} bytes <<<");
            }
            catch (Exception) // we de-synced 
            {
                Source.Synced = false;
                DatumCount = 0;
                BytesRead = 0;
                State = IoJobMeta.JobState.ConInvalid;
                DatumFragmentLength = 0;
                StillHasUnprocessedFragments = false;
            }
        }

        /// <summary>
        /// Updates buffer meta data
        /// </summary>
        public override void JobSync()
        {
            //Set how many datums we have available to process
            DatumCount = BytesLeftToProcess / DatumSize;
            DatumFragmentLength = BytesLeftToProcess % DatumSize;

            //Mark this job so that it does not go back into the heap until the remaining fragment has been picked up
            StillHasUnprocessedFragments = DatumFragmentLength > 0 && IoZero.SyncRecoveryModeEnabled && State == IoJobMeta.JobState.Consumed;
        }
    }
}
