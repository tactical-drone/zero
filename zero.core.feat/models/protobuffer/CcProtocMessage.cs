using System;
using System.Buffers;
using System.CodeDom;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using zero.core.conf;
using zero.core.misc;
using zero.core.network.ip;
using zero.core.patterns.bushings;
using zero.core.patterns.bushings.contracts;
using zero.core.patterns.misc;

namespace zero.core.feat.models.protobuffer
{
    public abstract class CcProtocMessage<TModel, TBatch> : IoMessage<CcProtocMessage<TModel, TBatch>>
    where TModel:IMessage where TBatch : class, IDisposable
    {
        protected CcProtocMessage(string sinkDesc, string jobDesc, IoSource<CcProtocMessage<TModel, TBatch>> source)
            : base(sinkDesc, jobDesc, source)
        {
            //sentinel
            if(Source == null)
                return;

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
            }
        }

        /// <summary>
        /// Message batch broadcast channel
        /// </summary>
        protected volatile IoConduit<CcProtocBatchJob<TModel, TBatch>> ProtocolConduit;

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
#endif
        }

        /// <summary>
        /// User data in the source
        /// </summary>
        protected byte[] RemoteEndPoint { get; } = new IPEndPoint(IPAddress.Any, 0).AsBytes();

        /// <summary>
        /// Produce a Message
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ioZero">The worker</param>
        /// <returns></returns>
        public override async ValueTask<IoJobMeta.JobState> ProduceAsync<T>(IIoSource.IoZeroCongestion<T> barrier, T ioZero)
        {
            try
            {
                await MessageService.ProduceAsync(static async (ioSocket, backPressure, ioZero, ioJob) =>
                {
                    var job = (CcProtocMessage<TModel, TBatch>)ioJob;
                    try
                    {
                        //----------------------------------------------------------------------------
                        // BARRIER
                        // We are only allowed to run ahead of the consumer by some configurable
                        // amount of steps. Instead of say just filling up memory buffers.
                        // This allows us some kind of (anti DOS?) congestion control
                        //----------------------------------------------------------------------------
                        if (!await backPressure(ioJob, ioZero).FastPath() || ioJob.Source.Zeroed())
                        {
                            _logger.Trace($"{nameof(backPressure)} [FAILED]: {ioJob.Description}");
                            await job.SetStateAsync(IoJobMeta.JobState.ProdCancel).FastPath();
                            return false;
                        }
                        //Async read the message from the message stream
                        //CryptographicOperations.ZeroMemory(job.MemoryBuffer.Span);
                        var read = await ((IoNetClient<CcProtocMessage<TModel, TBatch>>)ioSocket).IoNetSocket.ReadAsync(job.MemoryBuffer, job.BufferOffset, job.BufferSize, job.RemoteEndPoint).FastPath();
                        job.GenerateJobId();

                        //Drop zero reads
                        if (read == 0)
                        {
                            if (!job.MessageService.IsOperational() || ((IoNetClient<CcProtocMessage<TModel, TBatch>>)ioSocket).IoNetSocket.IsTcpSocket)
                            {
                                await job.MessageService.DisposeAsync(ioJob, "ZERO READS!!!").FastPath();
                                await job.SetStateAsync(IoJobMeta.JobState.Error).FastPath();
                            }
                            else
                            {
#if DEBUG
                                _logger.Error($"ReadAsync [FAILED]: ZERO UDP READS!!! {ioJob.Description}");
#endif
                                //await job.SetStateAsync(IoJobMeta.JobState.ProdConnReset).FastPath();
                                await job.SetStateAsync(IoJobMeta.JobState.ProdSkipped).FastPath();
                            }
                            return false;
                        }

                        Interlocked.Add(ref job.BytesRead, read);

#if TRACE
                        _logger.Trace($"<\\== {job.Buffer[job.DatumProvisionLengthMax..(job.DatumProvisionLengthMax + job.BytesRead)].PayloadSig("C")} {job.Description}; clr type = ({job.GetType().Name}), job id = {job.Id}, prev Id = {job.PreviousJob?.Id}, r = {job.BytesRead}, r = {job.BytesLeftToProcess}, dc = {job.DatumCount}, ds = {job.DatumSize}, f = {job.DatumFragmentLength}, b = {job.BytesLeftToProcess}/{job.BufferSize + job.DatumProvisionLengthMax}, b = {(int)(job.BytesLeftToProcess / (double)(job.BufferSize + job.DatumProvisionLengthMax) * 100)}%");
#endif

                        await job.SetStateAsync(IoJobMeta.JobState.Produced).FastPath();
                        return true;
                    }
                    catch when (job.Zeroed())
                    {
                        await job.SetStateAsync(IoJobMeta.JobState.Cancelled).FastPath();
                    }
                    catch (Exception e) when (!job.Zeroed())
                    {
                        await job.SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();
                        _logger.Error(e, $"ReadAsync {job.Description}:");
                    }

                    return false;
                }, this, barrier, ioZero).FastPath();
            }
            catch when (Zeroed()) { await SetStateAsync(IoJobMeta.JobState.Cancelled).FastPath();}
            catch (Exception e)when (!Zeroed())
            {
                _logger?.Warn(e, $"Producing job for {Description} returned with errors:");
            }
            finally
            {
                if (State == IoJobMeta.JobState.Producing)
                {
                    // Set the state to ProduceErr so that the consumer knows to abort consumption
                    await SetStateAsync(IoJobMeta.JobState.ProduceErr).FastPath();
                }
            }

            return State;
        }

        /// <summary>
        /// Synchronizes the byte stream, expects a frame = [ uint64 size, byte[] compressed_or_encrypted_payload ].
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

                return (int)magic;
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
    }
}
