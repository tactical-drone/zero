﻿using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;

namespace zero.core.feat.models
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
        //public IMemoryOwner<byte> MemoryOwner;
        public BrotliStream BufferBrotliStream;

        /// <summary>
        /// A buffer to receive the message in
        /// </summary>
        public byte[] Buffer;
        
        /// <summary>
        /// Array segment form
        /// </summary>
        public ArraySegment<byte> ArraySegment { get; protected set; }

        /// <summary>
        /// A memory buffer
        /// </summary>
        public Memory<byte> MemoryBuffer { get; protected set; }

        /// <summary>
        /// Stream access to the input
        /// </summary>
        public Stream ByteStream;

        /// <summary>
        /// Read only sequence wrapped for protobuf API
        /// </summary>
        public ReadOnlySequence<byte> ReadOnlySequence { get; protected set; }

        public ReadOnlySpan<byte> ReadOnlyMemory => new ReadOnlyMemory<byte>(Buffer).Span;

        /// <summary>
        /// The number of bytes read into the buffer
        /// </summary>
        protected int BytesRead;

        /// <summary>
        /// signature
        /// </summary>
        public abstract long Signature { get; protected set; }

        /// <summary>
        /// The number of bytes left to process in this buffer
        /// </summary>
        public int BytesLeftToProcess => BytesRead - (BufferOffset - DatumProvisionLengthMax);

        public int BytesFree => Buffer.Length - DatumProvisionLengthMax - BytesRead;

        /// <summary>
        /// The current offset
        /// </summary>
        public int BufferOffset;

        /// <summary>
        /// Total number of datums contained inside the buffer
        /// </summary>
        public int DatumCount;

        /// <summary>
        /// The number of bytes remaining of a fragmented datum
        /// </summary>
        public int DatumFragmentLength;

        /// <summary>
        /// The expected datum length
        /// </summary>
        public int DatumSize;

        /// <summary>
        /// The length of the buffer offset to allow previous fragments to be concatenated to the current buffer
        /// </summary>
        public int DatumProvisionLengthMax;

        /// <summary>
        /// Message receive buffer size
        /// </summary>        
        public int BufferSize;

        /// <summary>
        /// Prepares this item for use after being popped from the heap
        /// </summary>
        /// <param name="context"></param>
        /// <returns>This instance</returns>
        public override ValueTask<IIoHeapItem> HeapPopAsync(object context)
        {
            Interlocked.Exchange(ref BytesRead, 0);
            Interlocked.Exchange(ref BufferOffset, DatumProvisionLengthMax);
            Interlocked.Exchange(ref DatumFragmentLength, 0);
            return base.HeapPopAsync(context);
        }

        /// <summary>
        /// Does dup checking on this transaction
        /// </summary>        
        /// <returns></returns>
        public ValueTask<bool> WasProcessedRecentlyAsync(string key)
        {            
            return Source.RecentlyProcessed.KeyExistsAsync(key);
        }

        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

#if SAFE_RELEASE
            Buffer = null;
            ArraySegment = null;
#endif
        }

        /// <summary>
        /// Pulls previous fragment bytes into this job
        /// </summary>
        protected override async ValueTask AddRecoveryBitsAsync()
        {
            var p = (IoMessage<TJob>)PreviousJob;
            try
            {
                if (p.BytesLeftToProcess != p.BytesRead)
                {
                    if (p.DatumFragmentLength > DatumProvisionLengthMax)
                    {
                        Source.Synced = false;
                        Interlocked.Exchange(ref DatumCount, 0);
                        Interlocked.Exchange(ref BytesRead, 0);
                        await SetStateAsync(IoJobMeta.JobState.RSync).FastPath();

                        Interlocked.Exchange(ref DatumFragmentLength, 0);
                        return;
                    }

                    var bytesLeft = p.DatumFragmentLength;

                    Interlocked.Add(ref BufferOffset, -bytesLeft);

                    p.MemoryBuffer.Slice(p.BufferOffset, bytesLeft).CopyTo(MemoryBuffer[BufferOffset..]);
                }
                else
                {
                    var count = Math.Min(p.BytesLeftToProcess, BytesFree);
                    if (!p.MemoryBuffer[p.BufferOffset.. count].TryCopyTo(MemoryBuffer[BufferOffset..]))
                    {
                        Source.Synced = false;
                        Interlocked.Exchange(ref DatumCount, 0);
                        Interlocked.Exchange(ref BytesRead, 0);
                        await SetStateAsync(IoJobMeta.JobState.RSync).FastPath();

                        Interlocked.Exchange(ref DatumFragmentLength, 0);
                        return;
                    }

                    Interlocked.Add(ref BytesRead, count);
                }
            }
            catch when(Zeroed()){}
            catch (Exception e)when(!Zeroed())
            {
                _logger.Error(e,$"{nameof(AddRecoveryBitsAsync)}");
            }
        }

        /// <summary>
        /// If there are still <see cref="BytesLeftToProcess"/>, set status to syncing
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool ZeroEnsureRecovery()
        {
            try
            {
                Interlocked.Exchange(ref DatumCount, BytesLeftToProcess / DatumSize);
                Interlocked.Exchange(ref DatumFragmentLength, BytesLeftToProcess);

                Source.Synced = !(IoZero.ZeroRecoveryEnabled && DatumFragmentLength > 0);

                return !Source.Synced;
            }
            catch when(Zeroed()){}
            catch (Exception e) when(!Zeroed())
            {
                _logger.Error(e,$"{nameof(ZeroEnsureRecovery)}: Failed! {Description}");
            }

            return false;
        }
    }
}
