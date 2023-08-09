using System;
using System.Buffers;
using System.CodeDom;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.conf;
using zero.core.feat.models.bundle;
using zero.core.feat.models.protobuffer.sources;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.heap;
using zero.core.patterns.misc;
using Zero.Models.Protobuf;

namespace zero.core.feat.models.protobuffer
{
    public abstract class CcProtocMessage<TModel, TBatch> : IoMessage<CcProtocMessage<TModel, TBatch>>
    where TModel: class, IMessage where TBatch : class, IIoMessageBundle
    {
        protected CcProtocMessage(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<TModel, TBatch>> source)
            : base(sinkDesc, jobDesc, source)
        {
            Debug.Assert(parm_datums_per_buffer >=4);
            var blockSize = 8192;
            DatumSize = blockSize / parm_datums_per_buffer;
            DatumProvisionLengthMax = DatumSize;
            //Init buffers
            BufferSize = DatumSize * parm_datums_per_buffer; //SET to MTU x2 for decompression

            //MemoryOwner = MemoryPool<byte>.Shared.Rent(BufferSize + DatumProvisionLengthMax);
            //if (MemoryMarshal.TryGetArray((ReadOnlyMemory<byte>)MemoryOwner.Memory, out var malloc))
            {
                if (Buffer != null && Buffer.Length < BufferSize)
                {
                    throw new InternalBufferOverflowException($"Invalid buffer size of {BufferSize} < {Buffer.Length}");
                }
                ArraySegment = new ArraySegment<byte>(new byte[blockSize * 2]);

                Buffer = ArraySegment.Array;

                ReadOnlySequence = new ReadOnlySequence<byte>(Buffer);
                MemoryBuffer = new Memory<byte>(Buffer);
                ByteStream = new MemoryStream(Buffer!);
                BufferBrotliStream = new BrotliStream(new MemoryStream(Buffer, 0, ArraySegment.Count), CompressionMode.Decompress, true);
            }
        }

        /// <summary>
        /// Message batch broadcast channel
        /// </summary>
        protected IoConduit<CcProtocBatchJob<TModel, TBatch>> ProtocolConduit;

        /// <summary>
        /// Heap access
        /// </summary>
        public static IoHeap<TBatch, TModel> Heap => BatchHeap;

        /// <summary>
        /// Batch of messages
        /// </summary>
        protected TBatch CurrentBatch;

        /// <summary>
        /// Whether grouping by endpoint is supported
        /// </summary>
        private readonly bool _groupByEp;

        /// <summary>
        /// Batch item heap
        /// </summary>
        protected static IoHeap<TBatch, TModel> BatchHeap;

        /// <summary>
        /// Base source
        /// </summary>
        protected IoNetClient<CcProtocMessage<TModel, TBatch>> MessageService => (IoNetClient<CcProtocMessage<TModel, TBatch>>)Source;

        /// <summary>
        /// The time a consumer will wait for a source to release it before aborting in ms
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_producer_wait_for_consumer_timeout = 5000; //TODO make this adapting 

        /// <summary>
        /// Maximum number of datums this buffer can hold
        /// </summary>
        [IoParameter]
        // ReSharper disable once InconsistentNaming
        public int parm_datums_per_buffer = 4; // This must be at least 4


        /// <summary>
        /// How long to wait for the consumer before timing out
        /// </summary>
        public override int WaitForConsumerTimeout => parm_producer_wait_for_consumer_timeout;

        /// <summary>
        /// zero unmanaged
        /// </summary>
        public override void ZeroUnmanaged()
        {
            base.ZeroUnmanaged();

            //MemoryOwner.Dispose();
#if SAFE_RELEASE
            //MemoryOwner = null;
            ProtocolConduit = null;
            ArraySegment = null;
            Buffer = null;
            ReadOnlySequence = default;
            MemoryBuffer = null;
            ByteStream = null;
            CurrentBatch = null;
#endif
        }

        /// <summary>
        /// User data in the source
        /// </summary>
        protected byte[] RemoteEndPoint = new IPEndPoint(IPAddress.Any, 0).AsBytes();

        /// <summary>
        /// Produce a Message
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ioZero">The worker</param>
        /// <returns></returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(T ioZero)
        {
            try
            {
                if (await MessageService.ProduceAsync(static async (ioSocket, ioJob) =>
                    {
                        var job = (CcProtocMessage<TModel, TBatch>)ioJob;
                        try
                        {
                            //Async read the message from the message stream
                            //CryptographicOperations.ZeroMemory(job.MemoryBuffer.Span);
                            var read = await ((IoNetClient<CcProtocMessage<TModel, TBatch>>)ioSocket).IoNetSocket
                                .ReceiveAsync(job.MemoryBuffer, job.BufferOffset, job.BufferSize, job.RemoteEndPoint)
                                .FastPath();
                            job.GenerateJobId();

                            //Drop zero reads
                            if (read == 0)
                            {
                                if (ioSocket.Zeroed() || job.Zeroed() || job.IoZero.Zeroed())
                                    return false;

                                var socket = (IoNetClient<CcProtocMessage<TModel, TBatch>>)ioSocket;
                                if (!job.MessageService.IsOperational() || socket.IoNetSocket.IsTcpSocket)
                                {
                                    if (socket.IoNetSocket.IsTcpSocket)
                                    {
                                        await job.MessageService.DisposeAsync(ioJob,$"socket: {job.MessageService.IoNetSocket.LastError}").FastPath();
                                        await ioJob.SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();
                                        return false;
                                    }

                                    if (socket.IoNetSocket.LastError != SocketError.Success &&
                                        socket.IoNetSocket.LastError != SocketError.OperationAborted &&
                                        socket.IoNetSocket.LastError != SocketError.ConnectionReset)
                                    {
                                        await job.MessageService.DisposeAsync(ioJob,
                                            $"socket: {job.MessageService.IoNetSocket.LastError}").FastPath();
                                        await ioJob.SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();
                                        return false;
                                    }
                                }

                                if (!socket.IoNetSocket.IsTcpSocket && socket.IoNetSocket.LastError == SocketError.ConnectionReset)
                                {
                                    await ioJob.SetStateAsync(IoJobMeta.JobState.ProdSkipped).FastPath();
                                    return false;
                                }
#if DEBUG
                                _logger.Error($"ReceiveAsync [FAILED]: ZERO {socket.IoNetSocket.ProtocolType} READS!!! {ioJob.Description}");
#endif
                                await ioJob.SetStateAsync(IoJobMeta.JobState.ProdSkipped).FastPath();
                                await Task.Delay(250);
                                return false;
                            }

                            Interlocked.Add(ref job.BytesRead, read);
#if TRACE
                        _logger.Trace($"<\\== {job.Buffer[job.DatumProvisionLengthMax..(job.DatumProvisionLengthMax + job.BytesRead)].PayloadSig("C")} {job.Description}; clr type = ({job.GetType().Name}), job id = {job.Id}, prev Id = {job.PreviousJob?.Id}, r = {job.BytesRead}, r = {job.BytesLeftToProcess}, dc = {job.DatumCount}, ds = {job.DatumSize}, f = {job.DatumFragmentLength}, b = {job.BytesLeftToProcess}/{job.BufferSize + job.DatumProvisionLengthMax}, b = {(int)(job.BytesLeftToProcess / (double)(job.BufferSize + job.DatumProvisionLengthMax) * 100)}%");
#endif
                            return true;
                        }
                        catch when (job.Zeroed())
                        {
                        }
                        catch (Exception e) when (!job.Zeroed())
                        {
                            _logger.Error(e, $"ReceiveAsync {job.Description}:");
                        }

                        return false;
                    }, this).FastPath())
                {
                    return await SetStateAsync(IoJobMeta.JobState.Produced).FastPath();
                }
            }
            catch when (Zeroed())
            {
            }
            catch (Exception e)when (!Zeroed())
            {
                _logger?.Warn(e, $"Producing job for {Description} returned with errors:");
            }

            if(State == IoJobMeta.JobState.Producing)
                return await SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();

            return State;
        }

        /// <summary>
        /// Synchronizes the byte stream, expects a frame = [ uint64 size, int64 crc, byte[] compressed_or_encrypted_payload ].
        ///
        /// Sync scans for the inefficiencies in uint64 to find the frame start. It is assumed
        /// such an inefficiency wont exist in compressed/encrypted payloads. If it does, syncing
        /// might be sub optimal.
        ///
        /// This function should not run if byte streams contain verbatim data. 
        /// </summary>
#if RELEASE
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        protected int ZeroSync()
        {
            var read = 0;
            try
            {
                var magic = 0UL;
                while (read < BytesLeftToProcess && (magic = MemoryMarshal.Read<ulong>(ReadOnlyMemory[(BufferOffset + read)..])) > (ulong)BytesLeftToProcess)
                    read++;

                return (int)(magic & 0xFFFFFFFF); //TODO: investigate overflow errors
            }
            catch when(Zeroed()){}
            catch (Exception e)when (!Zeroed())
            {
                _logger.Error(e,$"${nameof(ZeroSync)}:");
            }
            finally
            {
                if(read > 0)
                    Interlocked.Add(ref BufferOffset, read);
            }

            return -1;
        }

        /// <summary>
        /// Processes a generic request and adds it to a batch
        /// </summary>
        /// <param name="packet">The packet</param>
        /// <typeparam name="T">The expected type</typeparam>
        /// <returns>The task</returns>
        protected async ValueTask ZeroBatchRequestAsync(chroniton packet)
        {
            try
            {
                var next = CurrentBatch.Feed;
                next.Zero = packet;

                RemoteEndPoint.CopyTo(next.EndPoint, 0);
                if (CurrentBatch.Flush)
                    await ZeroBatchAsync().FastPath();
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Error(e, $"{nameof(ZeroBatchRequestAsync)}");
            }
        }

        /// <summary>
        /// Forwards a batch of messages
        /// </summary>
        /// <returns>Task</returns>
        protected async ValueTask ZeroBatchAsync()
        {
            try
            {
                if (CurrentBatch.Count == 0 || Zeroed())
                    return;

                //cog the producer
                if (!await ProtocolConduit.Source.ProduceAsync(static (source, ioJob) =>
                {
                    var @this = (CcProtocMessage<TModel, TBatch>)ioJob;
                    try
                    {
                        var chan = ((CcProtocBatchSource<chroniton, TBatch>)source).Channel;
                        var nextBatch = Interlocked.Exchange(ref @this.CurrentBatch, BatchHeap.Take());

                        if (chan.Release(nextBatch, forceAsync: true) != 1)
                        {
                            var ready = chan.ReadyCount;
                            var wait = chan.WaitCount;
                            if (!((CcProtocBatchSource<chroniton, TBatch>)source).Zeroed() && !chan.Zeroed() && chan.TotalOps > 0)
                                _logger.Fatal($"{nameof(ZeroBatchAsync)}: Unable to q batch; had ready = {ready}, wait = {wait}; {chan.Description} {@this.Description}");

                            return new ValueTask<bool>(false);
                        }

                        if (@this.CurrentBatch == null)
                            throw new OutOfMemoryException($"{@this.Description}: {nameof(BatchHeap)}, c = {BatchHeap.Count}/{BatchHeap.Capacity}, ref = {BatchHeap.ReferenceCount}");

                        return new ValueTask<bool>(true);
                    }
                    catch (Exception) when (@this.Zeroed()) { }
                    catch (Exception e) when (!@this.Zeroed())
                    {
                        _logger.Error(e, $"{@this.Description} - Forward failed!");
                    }

                    return new ValueTask<bool>(false);
                }, this).FastPath())
                {
                    if (!Zeroed())
                        _logger.Trace($"{nameof(ZeroBatchAsync)}: Production [FAILED]: {ProtocolConduit.Description}");
                }
            }
            catch when (Zeroed()) { }
            catch (Exception e) when (!Zeroed())
            {
                _logger.Fatal(e, $"Forwarding from {Description} to {ProtocolConduit.Description} failed");
            }
        }
    }
}
